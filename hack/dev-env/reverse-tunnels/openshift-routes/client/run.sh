#!/bin/bash

SCRIPTPATH="$(
  cd -- "$(dirname "$0")" >/dev/null 2>&1 || exit
  pwd -P
)"

DOCKER_BIN=${DOCKER_BIN:-"docker"}

# Linux uses --network host, so 127.0.0.1 in frpc.toml is the principal, and :Z
# relabels the mounts for SELinux. macOS runs this container in a VM, where
# 127.0.0.1 is not the Mac and :Z fails with "lsetxattr: operation not supported".
RUN_ARGS=(--name frp-proxy -it --rm)
CONFIG_FILE="$SCRIPTPATH/frpc.toml"
VOLUME_SUFFIX=":Z"

if [[ "$OSTYPE" == darwin* ]]; then
  VOLUME_SUFFIX=""
  CONFIG_FILE="$(mktemp)"
  trap 'rm -f "$CONFIG_FILE"' EXIT
  sed 's/^localIP = "127.0.0.1"/localIP = "host.docker.internal"/' "$SCRIPTPATH/frpc.toml" > "$CONFIG_FILE"
  # Docker Desktop resolves host.docker.internal. Podman machine does not, and
  # host-gateway is empty there; gvproxy forwards 192.168.127.254 to the Mac.
  if "$DOCKER_BIN" --version 2>&1 | grep -qi podman; then
    RUN_ARGS+=(--add-host=host.docker.internal:192.168.127.254)
  fi
else
  RUN_ARGS+=(--network host)
fi

"$DOCKER_BIN" run "${RUN_ARGS[@]}" \
  -v "${CONFIG_FILE}:/app/config.toml${VOLUME_SUFFIX}" \
  -v "${SCRIPTPATH}/frp-ca.crt:/app/frp-ca.crt${VOLUME_SUFFIX}" \
  "docker.io/fatedier/frpc:v0.71.0@sha256:99ece6a2b62cfc68731e0df289af804ff1c699911cfc47871856434f1d6d53ee" \
  -c /app/config.toml

# or, without docker:
#
# frpc -c $SCRIPTPATH/config.toml
