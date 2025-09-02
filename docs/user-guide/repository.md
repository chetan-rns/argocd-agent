# Repository Management

This document explains how repository secrets work with Argo CD Agent and how repository access differs between agent modes.

## Overview

In Argo CD Agent, Git repository credentials are stored as Kubernetes Secrets with the label `argocd.argoproj.io/secret-type: repository`. Repository management varies by agent mode:

- **Managed agents**: Repositories are created on the control plane and distributed to agents that need them. The agents will handle the repository lifecycle events (create, update, delete) on the workload cluster.
- **Autonomous agents**: Repositories are created and managed locally on the workload cluster. The agents will not sync them back to the control plane.

### Managed Agent Mode

In managed mode, repositories are created on the **control plane** and automatically distributed to the workload clusters based on the project configuration.

**How it works:**

- Repository secrets **must include a `project` field** to be distributed to agents
- The repository is distributed to agents whose names match patterns in the AppProject's:
  - `.spec.destinations[].name` fields
  - `.spec.sourceNamespaces` fields
- Both pattern matches must succeed for an agent to receive the repository

**Repository-to-Agent Mapping:**

1. **Project Association**: Repository must specify which project it belongs to
2. **Pattern Matching**: Agent name must match AppProject destination and source namespace patterns (supports glob patterns like `agent-*`)
3. **Automatic Distribution**: Principal sends repository to all matching agents

**Example:** Creating a project-scoped repository:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: my-repo
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
type: Opaque
stringData:
  type: git
  url: https://github.com/myorg/myrepo.git
  username: myusername
  password: mytoken
  project: my-project  # Required: Associates repo with AppProject
EOF
```

This repository will be distributed to agents that match the patterns defined in the `my-project` AppProject's destinations and source namespaces.

**Important:** Repositories without a `project` field will **not** be distributed to any agents.

For more details about AppProject patterns and agent matching, see [AppProject Synchronization](appprojects.md).

### Autonomous Agent Mode

In autonomous mode, repositories are created and managed **locally on the workload cluster**. There is no synchronization back to the principal.

**How it works:**

- Create repository secrets directly on the agent cluster in the `argocd` namespace
- Argo CD uses these repositories locally for its applications
- Repository credentials remain local to the agent cluster

**Example:** Creating a repository on an autonomous agent:

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: my-repo
  namespace: argocd
  labels:
    argocd.argoproj.io/secret-type: repository
type: Opaque
stringData:
  type: git
  url: https://github.com/myorg/myrepo.git
  username: myusername
  password: mytoken
EOF
```

This repository will only be available to applications on this specific agent cluster.

## Common Operations

### Create

Repositories created on the control plane are automatically sent to the respective agents based on the AppProject's destination patterns

### Update

Updates to the repositories and the project configurations are synced with the appropriate target agents.

### Delete

Repositories deleted on the control plane are cleaned up from the workload clusters.

### Viewing Repositories

List repositories on the cluster:

```bash
kubectl get secrets -n argocd -l argocd.argoproj.io/secret-type=repository
```

## Troubleshooting

### Repository Not Distributed to Agents (Managed Mode)

If a repository created on the principal doesn't appear on managed agents:

1. **Check Project Scoping**: Ensure the repository has a `project` field:

   ```bash
   kubectl get secret my-repo -n argocd -o jsonpath='{.data.project}' | base64 -d
   ```

2. **Verify AppProject Patterns**: Confirm the AppProject's patterns match your agent names:

   ```bash
   kubectl get appproject my-project -n argocd -o yaml
   ```

   Check that agent names match both `.spec.destinations[].name` and `.spec.sourceNamespaces` patterns.

3. **Check Principal and Agent Logs**: Look for repository processing messages:

   ```bash
   kubectl logs -n argocd deployment/argocd-agent-principal
   ```

4. **Verify Agent Connectivity**: Ensure agents are connected and processing events:

   ```bash
   kubectl logs -n argocd deployment/argocd-agent | grep repository
   ```

For more information about Argo CD Agent configuration and other features, see:

- [Agent Configuration](../configuration/agent/configuration.md)
- [Application Management](applications.md)
- [AppProject Synchronization](appprojects.md)
