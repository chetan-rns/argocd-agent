## Scaling the Principal: Horizontal Sharding

_ai disclosure: diagrams and tables were formatted/generated with ai_

**Status:** Draft / discussion

## Overview

The principal is currently a single active process. At a few hundred workload clusters it becomes the throughput bottleneck for the whole fleet, and the existing HA replicas do not help: they are passive standbys for failure takeover, not additional capacity.

This proposal describes how to scale the principal horizontally by **sharding agents across multiple active principal instances**, while keeping a single Argo CD control plane — one UI, one set of Applications, one pane of glass.

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Unit of sharding | The agent | Each Application maps to exactly one agent, so per-agent ownership is a clean partition |
| Ownership store | The agent's existing Argo CD cluster secret | Already exists per agent, already watched by every principal — no new API object, no new datastore |
| Assignment policy | Explicit label, hash fallback | Zero config to start; explicit control for drain and migration |
| Agent steering | Connection-time redirect | Keeps mTLS termination at the principal; no L7 router in the credential path |
| Resource proxy routing | Per-shard address in the cluster secret | Already per-agent configuration — no Argo CD change |
| Redis proxy routing | Cross-shard forwarding inside the principal | Only path with a single fleet-wide address; avoids a new deployable |
| Default behaviour | `shardCount = 1` | Identical to today; sharding is strictly opt-in |

## Problem statement

Three distinct ceilings limit the principal today. Only one of them is addressed by adding processes, which is why this proposal pairs sharding with in-process fixes.

### Passive replicas contribute no capacity

Per `docs/concepts/ha.md`, only the ACTIVE principal returns 200 on `/healthz`, and the GSLB routes agents exclusively there. A replica's `ShouldAcceptAgents()` returns false. Fleet throughput therefore equals one principal's throughput, regardless of replica count.

### In-process serialization

A single principal saturates well below its available hardware:

- **`principal/event.go:942`** — `eventProcessor` is one goroutine that scans *every* agent queue in a loop, sleeping 10ms when idle, dispatching through a process-wide semaphore of **10** workers (`principal/options.go:133`). At 500 agents that is ten workers for the entire fleet plus a scan loop that spends most of its time walking empty queues.
- **`principal/server.go:124`** — `watchLock`, a global `RWMutex` acquired in `updateAppCallback`. The in-code comment already identifies it as "a short-term attempt to preserve update event ordering… known to be problematic." It serializes Application updates across all agents.
- `queueLock.TryLock` permits one in-flight item per agent queue. This is correct for FIFO ordering, but it means per-agent throughput is bounded by apiserver round-trip latency.

### Fleet-wide watch and write load

`internal/backend/kubernetes/application/kubernetes.go:95` lists `Applications("")` — cluster-wide. The `--namespaces` option (`principal/server.go:1400`) is a *post-hoc filter* in the callback chain, not a watch scope. Every principal therefore caches every Application in the fleet (memory O(total apps)), and every status update from every agent becomes a write against the control-plane apiserver.

Sharding addresses the first ceiling and most of the third. It does **not** address the second — and sharding without fixing the second requires far more shards than necessary.

## Architecture

```text
                        Agents (workload clusters)
   [agent-a]      [agent-b]      [agent-c]      ...      [agent-z]
       │              │              │                        │
       │   1. dial shared endpoint principal.example.com:443  │
       └──────────────┴──────┬───────┴────────────────────────┘
                             │
                   2. non-owner replies FAILED_PRECONDITION
                      + owner endpoint; agent redials
                             │
   ┌─────────────────────────┼─────────────────────────────────┐
   │              CONTROL PLANE CLUSTER                        │
   │                                                           │
   │  ┌───────────┐   ┌───────────┐   ┌───────────┐            │
   │  │ principal │   │ principal │   │ principal │            │
   │  │  shard 0  │   │  shard 1  │   │  shard 2  │            │
   │  │           │   │           │   │           │            │
   │  │ gRPC      │   │ gRPC      │   │ gRPC      │            │
   │  │ res-proxy │   │ res-proxy │   │ res-proxy │            │
   │  │ redis-pxy │◄─►│ redis-pxy │◄─►│ redis-pxy │  forward   │
   │  └─────┬─────┘   └─────┬─────┘   └─────┬─────┘            │
   │        │               │               │                  │
   │        └───────────────┴───────────────┘                  │
   │                        │                                  │
   │             ┌──────────▼───────────┐                      │
   │             │   Argo CD (single)   │  ← single pane of    │
   │             │   server / repo /    │    glass preserved   │
   │             │   app-controller     │                      │
   │             └──────────┬───────────┘                      │
   │                        │                                  │
   │             ┌──────────▼───────────┐                      │
   │             │  Control-plane API   │                      │
   │             │  Applications,       │                      │
   │             │  cluster secrets     │                      │
   │             └──────────────────────┘                      │
   └───────────────────────────────────────────────────────────┘
```

Each shard owns a disjoint subset of agents. Argo CD is unaware that more than one principal exists.

## Design

### Ownership

A new `internal/sharding` package exposes a single predicate:

```go
func (s *Sharder) Owns(agentName string) bool
```

Resolution order:

1. `argocd-agent.argoproj-labs.io/shard: "<n>"` label on the agent's Argo CD cluster secret — explicit pin.
2. Otherwise `jumpHash(agentName, shardCount)`.

"Hash by default, label overrides" gives zero-config startup plus explicit operator control for drain, pinning, and gradual migration.

The cluster secret is the right home for this. It already exists per agent, already carries an agent-name label (`LabelKeyClusterAgentMapping`), and is already watched by every principal via `internal/argocd/cluster/manager.go`. Every shard can therefore compute every ownership decision locally, with no coordination service and no new runtime dependency — consistent with the project's "no mandatory heavy runtime dependency" design principle.

Shard identity comes from a StatefulSet ordinal: `--shard-index` derived from the pod name, `--shard-count` from a flag or ConfigMap. `shardCount = 1` is the default and is behaviourally identical to today.

`Owns()` is wired into four hook points that already exist:

| Hook | Location | Effect |
|------|----------|--------|
| `eventstream.WithAcceptCheck` | `principal/listen.go:278` | Refuse agents this shard does not own |
| `defaultAppFilterChain` | `principal/server.go:1400` | Ignore Applications belonging to other shards' agents |
| `defaultAppProjectFilterChain` | `principal/server.go:1430` | Same, for AppProjects |
| Cluster manager callbacks | `internal/argocd/cluster/manager.go` | Map only owned agents |

The accept-check hook already exists because HA needed it.

### Steering agents to their shard

**Connection-time redirect.** The agent keeps a single configured address. It dials the shared Service, lands on an arbitrary shard, and that shard evaluates ownership. If it is not the owner it fails `Subscribe` with `FAILED_PRECONDITION` and the owner's address in the status details. The agent's existing reconnect/backoff loop dials that address and caches it.

Per-shard addressability comes from a headless StatefulSet Service in-cluster. For external agents, the redirect address comes from operator configuration rather than inferred pod DNS:

```
--shard-endpoint-template=principal-%d.example.com:443
```

**Why not an L7 router.** A router in front would have to route on the agent's TLS SNI or client-certificate CN. Routing on CN requires terminating agent mTLS at the router, which breaks the authentication model in `internal/auth` and creates a credential-handling chokepoint. Routing on SNI only works if agents set a distinguishing SNI, which is the same configuration burden as the redirect with none of the simplicity. The redirect keeps all mTLS termination at the principal.

**Invariant: a non-owner must refuse, never serve.** The moment two shards serve the same agent, duplicate Application writes and event ping-pong follow.

### Control plane to agent paths

#### Resource proxy — live resources, pod logs, web terminal

This path is nearly free, because its address is not global: it is stored per agent in the cluster secret's `Server` field, set by `agentctl` via `--resource-proxy-server` (`cmd/ctl/agent.go:257`). Give each shard its own resource-proxy Service and point each agent's cluster secret at its owner. **No Argo CD change.** The web terminal (`docs/technical/web-terminal-workflow.md`) rides along for free, since it is a Kubernetes exec call against that same URL.

The caveat: the cluster `server` URL is the Argo CD cluster identity. If Applications use `spec.destination.server`, changing it on rebalance breaks them. Two resolutions:

- **Require `destination.name`** (cluster name = agent name). With destination-based mapping enabled this is already the routing key (`principal/callbacks.go:1030-1032`), so the server URL becomes an implementation detail free to change per shard. This also aligns with the ongoing deprecation of namespace-based mapping.
- Or maintain a **stable per-agent DNS name** whose Endpoints the owning shard reconciles. No Application changes, but it adds N Endpoints objects and a reconcile loop.

**Recommendation:** adopt the first and gate sharding on destination-based mapping.

#### Redis proxy — the only fleet-wide address

The redis proxy is a single address in Argo CD's configuration (`install/kubernetes/principal/principal-redis-proxy-service.yaml`), consumed by argocd-server, repo-server, and the application controller.

Keep the redis proxy in every principal pod and make it **forward across shards**. A shard receiving a RESP command for an application it does not own resolves the owner using the same `Owns()` predicate over the same cluster-secret data it already watches, then proxies the connection to that shard's redis-proxy port. The existing "not an agent application → principal redis" path is untouched.

This costs one extra hop and adds no new deployable and no Argo CD configuration change. A standalone stateless redis-router Deployment is cleaner separation and can follow later if the hop shows up in UI latency.

#### Everything else

Registration, token issuance, `/healthz`, metrics, and the HA admin endpoint stay per-shard and stateless. Any shard can serve registration, since it only writes a cluster secret.

### Reducing fleet-wide watch load

Sharding agents does not automatically shard informers, and this is where the memory goes.

- **Managed mode:** Applications live in per-agent namespaces, so a shard with a bounded namespace set should construct **per-namespace informers** rather than `Applications("")` plus a filter. This is the robust path — users create these Applications, so a label cannot be assumed present.
- **Autonomous mode:** the principal creates the Applications itself, so it can stamp a shard label and use the existing `WithLabelSelector` option (`internal/backend/kubernetes/application/kubernetes.go:74`) as a *watch* selector. The plumbing already exists.

`populateSourceCache` (`principal/server.go:1512`) must also be scoped to owned agents, or shard startup cost stays O(fleet).

### The apiserver ceiling

Sharding does not reduce total writes against the control-plane apiserver — it increases the client count. At hundreds of clusters with hundreds of Applications each, etcd write throughput is plausibly the real wall, and more principals makes it worse unless accompanied by:

- per-Application status-update coalescing (collapse N updates in a window into one),
- no-op suppression via resourceVersion or semantic comparison before writing,
- a configurable `--app-status-update-interval`.

This should be measured independently of sharding. It may be the highest-value item in this document.

## Prerequisite: in-process fixes

These are small, independently valuable, and reduce the number of shards required. They should land before sharding.

1. **Replace the busy-poll event loop.** One goroutine per agent queue doing a blocking `workqueue.Get()`, replacing the O(N) scan, the 10ms sleep, and the global semaphore of 10 (`principal/event.go:942-1031`). FIFO per agent is preserved because each agent has its own goroutine; the fleet-wide concurrency cap and the idle-scan CPU burn both disappear.
2. **Narrow `watchLock`** (`principal/server.go:124`) to the existing per-resource `namedlock` (`principal/server.go:200`) so updates for different agents stop serializing.
3. **Instrument.** Per-agent queue depth, event-handling latency histogram, apiserver write QPS, informer cache size. Without these it is impossible to tell whether a shard is saturated or merely waiting on etcd.

Expectation: (1) and (2) alone move the needle substantially, and (3) determines how many shards are actually needed.

## Risks and failure modes

**Dual ownership.** During a resize, or with a stale cluster-secret label, two shards can briefly both accept an agent, producing duplicate writes and conflicting status. Mitigations: a single source of ownership truth; non-owners hard-refuse; the agent's single gRPC stream is itself a serialization point. If server-side apply is adopted, **all shards must use the same field-manager name** — distinct managers would make concurrent writes conflict rather than converge. There is no airtight answer without a lease; the design accepts a short window and relies on idempotent writes.

**Rebalance storms.** Growing from 4 to 5 shards with jump hash relocates roughly 20% of agents, and each relocated agent runs `handleResyncOnConnect` → `sendCurrentStateToAgent` (`principal/server.go:1024`, `:1110`). That is a thundering herd against both the apiserver and the agents. This is the argument for explicit label assignment with **no automatic rebalance**: an `agentctl shard move` that drains one shard at a time, with jittered reconnect.

**Redirect loops.** An unresolvable or firewalled shard endpoint makes an agent bounce between the shared endpoint and a dead shard indefinitely. Validate all shard endpoints at startup, expose `principal_redirects_total{outcome}`, and define the policy explicitly: refuse, emit a metric, let the agent retry — **not** "serve it locally anyway", which is dual ownership by another name.

**Partial global views.** `repoToAgents` and `projectToRepos` (`principal/server.go:116`, `:119`) become per-shard partial views. Any logic of the form "is this repository still referenced by anyone?" becomes incorrect. These call sites need an audit; decisions must become owner-local or be recomputed from the API.

**Fleet-global objects.** AppProjects, repositories, the blocklist, settings, GPG keys, and JWT signing keys are watched by every shard. This is correct behaviour — each shard pushes only to its own agents — but it is N× watch load on those objects. Small, but worth knowing.

**Ordering.** Today a single process serializes an Application's events. Across shards, safety depends on each Application mapping to exactly one agent, which `getAgentNameForApp` (`principal/callbacks.go:1025`) guarantees in both mapping modes. This warrants an explicit test so a future mapping change cannot silently break per-agent ownership.

**Cross-shard redis hop.** Adds latency to every UI resource-tree load and concentrates connections on the owning shard. Pool and multiplex inter-shard connections, cache the agent-to-shard lookup, and expose metrics for forwarded versus local requests.

**Security.** No new trust boundary at the agent edge: each shard terminates its own agents' mTLS and no shard proxies another's agent credentials. Two new surfaces require attention — the inter-shard redis channel (mTLS, plus extending `principal-redis-proxy-networkpolicy.yaml`), and the shard label on cluster secrets as a new privileged input. The marginal risk of the latter is low, since anyone able to edit cluster secrets can already do worse, but it should be RBAC-restricted to the principal service account and administrators.

**Operability.** "Which principal is my agent on?" becomes a real question. Required: a `shard` field on every log line, a `principal_agent_shard` metric, shard information in `agentctl agent get`, and a fan-out mode for cross-shard `agentctl` queries. `docs/operations/metrics.md` needs updating — existing dashboards assume a single instance.

**Interaction with HA.** The cleanest v1 is to declare sharding and cross-region HA **mutually exclusive and document it**, rather than half-implementing. The eventual model is per-shard active/passive — shard *k* in region A replicating to shard *k* in region B, with matched shard counts and per-shard or promote-all failover — but the replication client in `principal/ha_integration.go` must become shard-aware first, which is a meaningful chunk of work.

**Test surface.** `hack/dev-env` and the e2e suite assume one principal. A multi-shard dev-env target and new e2e coverage for redirect, rebalance, and cross-shard redis are prerequisites for trusting any of this.

## Phasing

| Phase | Work | Exit criterion |
|-------|------|----------------|
| 0 | Benchmark and instrument | The saturating resource is known |
| 1 | Event loop and `watchLock` fixes | Re-measured; may be sufficient alone |
| 2 | `internal/sharding` + `Owns()` wiring, `shardCount=1` default | No behavioural change, fully shipped |
| 3 | Redirect, per-shard endpoints, shard label, multi-shard dev-env and e2e | Two shards work end to end |
| 4 | Cross-shard redis forwarding | Argo CD UI works across shards |
| 5 | Informer scoping and write coalescing | Per-shard memory and apiserver QPS drop |
| 6 | Shard-aware HA replication, `agentctl shard` tooling, docs | Sharding and HA coexist |

Phases 1 and 2 are independently shippable and carry no behavioural risk. Everything from phase 3 on is incremental.

## Alternatives considered

**Stateless principals over a shared message bus** (NATS or Kafka — the direction opened by the pluggable-transport work). Best long-term throughput, but it introduces a mandatory heavy runtime dependency, contradicting a stated design principle, and the resource proxy and web terminal still need to reach the specific pod holding the agent's stream. Sharding is a prerequisite for this rather than an alternative to it.

**Vertical scaling only.** The global locks and single event loop mean vertical scaling plateaus, and it provides no fault isolation — one principal OOM still takes out the whole fleet.

**Active-active across all agents with per-Application distributed locking.** Requires a consensus store for marginal benefit over sharding.

**Multiple Argo CD control planes.** Loses the single pane of glass, which is the property this proposal exists to protect.

## Open questions

- Which resource actually saturated in recent performance testing — CPU, apiserver QPS, queue depth, event latency, or RSS? If it is apiserver writes, the write-coalescing work outranks sharding entirely.
- Target scale: number of agents, Applications per agent, and status update rate.
- Can agents reach N distinct endpoints, or is exactly one DNS name a hard requirement? If the latter, the steering design must fall back to an L7 router and becomes considerably more complex.
- Do deployments use `destination.name` or `destination.server`? This determines whether resource-proxy routing is nearly free or needs the per-agent Endpoints machinery.
- Must sharding coexist with cross-region HA in v1?
