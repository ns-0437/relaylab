#!/usr/bin/env bash
# Phase 3A Part B: shared constants/helpers, sourced by every script in
# infra/k8s/scripts/. Nothing here performs an action on its own.
set -euo pipefail

# kind was installed via `winget install Kubernetes.kind`; winget's PATH
# update requires a shell restart to take effect, which this script
# cannot assume has happened. Add the known install directory if `kind`
# is not already resolvable, so these scripts work in a fresh shell too.
if ! command -v kind >/dev/null 2>&1; then
  WINGET_KIND_DIR="/c/Users/$(whoami)/AppData/Local/Microsoft/WinGet/Packages/Kubernetes.kind_Microsoft.Winget.Source_8wekyb3d8bbwe"
  if [ -x "$WINGET_KIND_DIR/kind.exe" ]; then
    export PATH="$WINGET_KIND_DIR:$PATH"
  fi
fi
command -v kind >/dev/null 2>&1 || { echo "[relaylab-k8s] ERROR: 'kind' not found on PATH and not found at the known winget install directory." >&2; exit 1; }

CLUSTER_NAME="relaylab-local"
NAMESPACE="relaylab"
IMAGE_TAG="relaylab-app:local"

# Deliberately DIFFERENT from infra/compose.yaml's host ports (3306,
# 24222) -- both stacks can be running on this host at once (confirmed:
# the compose dev stack was already up while this was built), and
# `kubectl port-forward` binding a DIFFERENT free port number, rather
# than risking an accidental same-port bind on a different address
# family (observed: compose's mysql bound 127.0.0.1:3306 while
# `kubectl port-forward 3306:3306` happily bound [::1]:3306 instead of
# failing -- which would have made a "127.0.0.1:3306" connection silently
# reach the WRONG database), is what actually guarantees no ambiguity.
K8S_MYSQL_HOST_PORT=33060
K8S_NATS_HOST_PORT=24322

K8S_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"   # infra/k8s (common.sh lives at infra/k8s/scripts/lib/)
REPO_ROOT="$(cd "$K8S_DIR/../.." && pwd)"
KUBECONFIG_FILE="$K8S_DIR/.kubeconfig"          # dedicated, gitignored -- never touches ~/.kube/config
SECRETS_FILE="$K8S_DIR/.secrets.env"            # generated, gitignored
PORT_FORWARD_PID_FILE="$K8S_DIR/.port-forward-pids"

export KUBECONFIG="$KUBECONFIG_FILE"

kctl() {
  kubectl --kubeconfig "$KUBECONFIG_FILE" --context "kind-$CLUSTER_NAME" --namespace "$NAMESPACE" "$@"
}

log() { echo "[relaylab-k8s] $*"; }
fail() { echo "[relaylab-k8s] ERROR: $*" >&2; exit 1; }

# `kind load docker-image` (which runs `ctr images import --all-platforms`
# internally) fails on this host for vendor multi-platform images
# (mysql:8.4, nats:2.10-alpine) with "content digest ... not found" --
# Docker Desktop's containerd image store holds a multi-platform manifest
# list but only this host's own platform's layer content, and
# --all-platforms then tries to import platforms with no local content.
# `docker save` (single-platform, matching the local daemon) + a plain
# `ctr images import` (no --all-platforms) avoids the bug entirely.
# Separately observed on this host: letting the kind node's kubelet pull
# these images itself from Docker Hub hangs indefinitely (confirmed via a
# direct `crictl pull mysql:8.4` inside the node, >90s with no progress) --
# this function also avoids that path.
load_vendor_image() {
  local image="$1"
  local safe_name
  safe_name="$(echo "$image" | tr '/:' '__')"
  local tar_path="/tmp/relaylab-k8s-${safe_name}.tar"
  log "loading vendor image '$image' into kind node (docker save + ctr import, bypassing both the slow in-cluster pull and kind's own --all-platforms bug) ..."
  docker save "$image" -o "$tar_path"
  docker cp "$tar_path" "${CLUSTER_NAME}-control-plane:/relaylab-import.tar"
  MSYS_NO_PATHCONV=1 docker exec "${CLUSTER_NAME}-control-plane" ctr --namespace=k8s.io images import /relaylab-import.tar >/dev/null
  MSYS_NO_PATHCONV=1 docker exec "${CLUSTER_NAME}-control-plane" rm -f /relaylab-import.tar
  rm -f "$tar_path"
}

# Phase 3A.1: a real orphaned-process incident, found while validating
# this very patch -- THREE stray `kubectl port-forward` processes from
# earlier, unrelated script runs were still bound to this project's
# exact ports (33060, 24322, 33061) despite every script's own PID
# tracking/trap cleanup, because each had become detached from the
# shell that started it (observed via `netstat -ano` + Windows
# `taskkill`, not via this project's own PID files, which no longer
# referenced them). A PID file alone cannot catch this -- it only tracks
# what ITS OWN run started. This preflight checks the ACTUAL port state
# before binding anything new, so a leftover squatter fails loudly and
# immediately instead of forcing a confusing bind error (or, worse,
# causing kubectl to silently fall back to a different address family --
# see the --address 127.0.0.1 comments elsewhere in this directory).
require_port_free() {
  local port="$1" label="$2"
  if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
    exec 3>&-
    local pid=""
    if command -v netstat >/dev/null 2>&1; then
      pid="$(netstat -ano 2>/dev/null | grep "127.0.0.1:${port} " | grep LISTENING | awk '{print $NF}' | head -1)"
    fi
    fail "port 127.0.0.1:${port} ($label) is already in use${pid:+ by PID $pid}" \
"-- this is almost certainly a leftover process from an earlier run, not a real conflict with unrelated \
software (this port is project-owned). Free it first (Windows: 'taskkill //PID <pid> //F'; this project's \
own stop-port-forwards.sh only stops PIDs it itself recorded, so an orphan from a DIFFERENT run will not \
be found there) and re-run."
  fi
}

# Phase 3A.1: `kubectl apply` only triggers a Deployment rollout when the
# POD TEMPLATE actually differs -- a locally built image reused under
# the SAME floating tag (`relaylab-app:local`) does NOT change that
# template string even when its CONTENT changed (confirmed directly:
# re-applying 40/50-*.yaml after rebuilding left the exact same pod names
# running the OLD image). Stamping the resolved image ID onto the pod
# template as an annotation makes the template genuinely differ whenever
# the image content differs, so a rollout happens automatically and
# `kubectl rollout status` has something real to wait on -- the smallest
# fix that makes "re-run setup" actually redeploy a rebuilt image,
# without introducing image digests/registries this phase doesn't use.
# Phase 3A.1: a real reliability gap found WHILE validating this very
# patch -- plain `kill "$pid"` against a backgrounded `kubectl
# port-forward` repeatedly left the underlying Windows process alive as
# an orphan (observed directly, multiple times, via `netstat -ano` after
# a script's own `kill` call had already run and reported no error).
# This Git-Bash/MSYS environment's `kill` does not reliably reach a
# native Win32 process the way POSIX SIGTERM would on Linux/macOS.
# `taskkill //F` against the SAME pid is the actually-reliable
# mechanism here; `kill` is tried first (cheap, sufficient on a real
# POSIX host) and `taskkill` is the fallback, never the other way
# around, so this stays a no-op cost on a non-Windows host.
kill_pid_robust() {
  local pid="$1"
  kill "$pid" 2>/dev/null || true
  # Always ALSO tries taskkill, unconditionally -- do not gate this on a
  # `kill -0 "$pid"` check first: that check can itself report "not
  # alive" for a bash-side job handle while the real underlying Windows
  # process (a different, or the same, PID from this environment's
  # perspective) is still running, which is exactly the false negative
  # observed in practice. taskkill against an already-gone PID just
  # errors harmlessly (suppressed below).
  if command -v taskkill >/dev/null 2>&1; then
    taskkill //PID "$pid" //F >/dev/null 2>&1 || true
  fi
}

# Phase 3A.1: even `kill_pid_robust` (PID + taskkill) was observed to
# miss, in practice -- a `kubectl port-forward` backgrounded from Git
# Bash is not always the SAME Windows PID bash's `$!` reports (an
# intermediate wrapper is sometimes what `$!` actually names). The one
# mechanism that reliably worked in every case actually hit while
# validating this patch: look up whatever PID the OS itself reports as
# LISTENING on the exact port, and taskkill THAT -- it is unambiguous by
# construction, independent of whatever bash's job control believes.
kill_port_robust() {
  local port="$1"
  if ! command -v netstat >/dev/null 2>&1 || ! command -v taskkill >/dev/null 2>&1; then
    return 0
  fi
  local pid
  pid="$(netstat -ano 2>/dev/null | grep "127.0.0.1:${port} " | grep LISTENING | awk '{print $NF}' | head -1)"
  if [ -n "$pid" ]; then
    taskkill //PID "$pid" //F >/dev/null 2>&1 || true
  fi
}

stamp_and_rollout() {
  local deployment="$1" image_id="$2"
  kctl patch deployment "$deployment" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"relaylab.io/image-id\":\"${image_id}\"}}}}}" >/dev/null
}

require_cluster_exists() {
  # `kind get clusters` lists by Docker container labels, not by
  # kubeconfig -- it never takes --kubeconfig.
  if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    fail "kind cluster '$CLUSTER_NAME' does not exist. Run 02-up.sh first."
  fi
}

# Phase 3A.1: `kubectl wait --for=condition=Ready` on its own fails with
# only "timed out waiting for the condition" -- not actionable. On
# failure, this dumps pod status, recent events, and recent logs for the
# matching pods before failing loudly, so the FIRST error message already
# points at the real cause instead of requiring a second manual
# `kubectl describe` round-trip.
wait_ready_or_diagnose() {
  local label="$1"
  local timeout="$2"
  if kctl wait --for=condition=Ready pod -l "$label" --timeout="$timeout"; then
    return 0
  fi
  log "DIAGNOSTICS for pod -l $label (readiness wait failed):"
  kctl get pods -l "$label" -o wide || true
  kctl describe pod -l "$label" | tail -n 30 || true
  for pod in $(kctl get pods -l "$label" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    log "--- recent logs: $pod ---"
    kctl logs "$pod" --tail=30 --all-containers 2>&1 || true
  done
  fail "pod -l $label did not become Ready within $timeout -- see diagnostics above."
}

# Phase 3A.1: read the MySQL app credentials back out of the Secret that
# is ACTUALLY applied to the cluster right now (not out of the local
# secrets file), for a byte-for-byte mismatch check. base64 -d is
# POSIX/coreutils, available in this project's Git Bash environment.
read_cluster_mysql_secret_field() {
  kctl get secret relaylab-mysql-credentials -o jsonpath="{.data.$1}" 2>/dev/null | base64 -d 2>/dev/null || true
}

# Phase 3A.1: the actual PVC-deletion incident (see docs/KUBERNETES.md
# "Setup re-entry and credential safety") was caused by debugging a stuck
# pod, not by this setup script -- but nothing previously stopped a
# REGENERATED local secrets file from silently overwriting the cluster's
# Secret with a password that no longer matches what an ALREADY-
# INITIALIZED MySQL data directory was actually given (MYSQL_USER/
# MYSQL_PASSWORD only take effect on the data directory's FIRST
# initialization -- changing the Secret afterward does not change what
# MySQL itself will authenticate). This function is the fix: it never
# blindly re-applies generated values over an existing cluster Secret.
ensure_mysql_secret() {
  local pvc_exists=0 secret_exists=0
  kctl get pvc data-relaylab-mysql-0 >/dev/null 2>&1 && pvc_exists=1
  kctl get secret relaylab-mysql-credentials >/dev/null 2>&1 && secret_exists=1

  if [ -f "$SECRETS_FILE" ]; then
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
    if [ "$secret_exists" -eq 1 ]; then
      local cluster_pw
      cluster_pw="$(read_cluster_mysql_secret_field app-password)"
      if [ "$cluster_pw" != "$MYSQL_APP_PASSWORD" ]; then
        fail "the local secrets file ($SECRETS_FILE) does NOT match the credentials already applied to the \
cluster's relaylab-mysql-credentials Secret. Re-applying the local file's values would desynchronize the \
Secret from what publisher/consumer/migrate actually authenticate with, and -- if the MySQL PVC is already \
initialized -- from the password baked into MySQL's own user table (MYSQL_PASSWORD only takes effect on the \
data directory's FIRST initialization, never after). Restore the original $SECRETS_FILE, or run teardown.sh \
first if you intend to start over with fresh storage and fresh credentials."
      fi
      log "local secrets file matches the cluster's existing Secret -- reusing without changes."
      return 0
    fi
    if [ "$pvc_exists" -eq 1 ]; then
      log "WARNING: the MySQL PVC already exists but relaylab-mysql-credentials does not -- applying this Secret \
assumes its password matches what MySQL was already initialized with. If this cluster was set up with a \
DIFFERENT secrets file, authentication will start failing; restore the original file instead if so."
    fi
  else
    if [ "$secret_exists" -eq 1 ]; then
      log "no local secrets file found, but the cluster's Secret already exists -- reading it back into \
$SECRETS_FILE instead of generating new, incompatible credentials."
      MYSQL_ROOT_PASSWORD="$(read_cluster_mysql_secret_field root-password)"
      MYSQL_APP_USER="$(read_cluster_mysql_secret_field app-user)"
      MYSQL_APP_PASSWORD="$(read_cluster_mysql_secret_field app-password)"
      [ -n "$MYSQL_APP_PASSWORD" ] || fail "failed to read back relaylab-mysql-credentials from the cluster -- refusing to proceed without known-good credentials."
      { echo "MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD"; echo "MYSQL_APP_USER=$MYSQL_APP_USER"; echo "MYSQL_APP_PASSWORD=$MYSQL_APP_PASSWORD"; } > "$SECRETS_FILE"
      chmod 600 "$SECRETS_FILE"
      return 0
    fi
    if [ "$pvc_exists" -eq 1 ]; then
      fail "the MySQL PVC already exists (likely contains initialized data) but NEITHER the cluster's Secret \
nor a local secrets file exists -- generating a brand new random password here would not match whatever MySQL \
was actually initialized with, and every subsequent connection would fail authentication. Recover the original \
secrets file, or run teardown.sh to start over with fresh storage."
    fi
    log "generating local dev secrets (never committed -- see .gitignore) at $SECRETS_FILE"
    { echo "MYSQL_ROOT_PASSWORD=$(openssl rand -hex 16)"; echo "MYSQL_APP_USER=relaylab_dev"; echo "MYSQL_APP_PASSWORD=$(openssl rand -hex 16)"; } > "$SECRETS_FILE"
    chmod 600 "$SECRETS_FILE"
    # shellcheck disable=SC1090
    source "$SECRETS_FILE"
  fi

  if [ "$secret_exists" -eq 0 ]; then
    log "applying relaylab-mysql-credentials Secret (first time for this cluster) ..."
    kctl create secret generic relaylab-mysql-credentials \
      --from-literal=root-password="$MYSQL_ROOT_PASSWORD" \
      --from-literal=app-user="$MYSQL_APP_USER" \
      --from-literal=app-password="$MYSQL_APP_PASSWORD" >/dev/null
  fi
}

# Phase 3A.1: positively confirms a demo/migration connection actually
# reached the CLUSTER's MySQL, not infra/compose.yaml's dev MySQL --
# both run the identical `mysql:8.4` image, so server version alone
# cannot distinguish them; `@@hostname` can, since it is the container's
# own hostname (the StatefulSet pod's name, "relaylab-mysql-0", for the
# cluster; something else entirely for compose). Fails loudly rather
# than silently proceeding against the wrong database.
assert_targeting_cluster_mysql() {
  local host="$1" port="$2" user="$3" password="$4" database="$5"
  local hostname
  hostname="$(cd "$REPO_ROOT" && node -e "
const mysql = require('mysql2/promise');
(async () => {
  const conn = await mysql.createConnection({ host: '$host', port: $port, user: '$user', password: '$password', database: '$database' });
  try {
    const [rows] = await conn.query('SELECT @@hostname AS h');
    process.stdout.write(String(rows[0].h));
  } finally {
    await conn.end();
  }
})().catch((err) => { console.error(err.message); process.exit(1); });
" 2>/tmp/relaylab-hostname-check.log)" || fail "could not query the target MySQL's @@hostname -- see /tmp/relaylab-hostname-check.log"
  case "$hostname" in
    relaylab-mysql-*) log "confirmed: connected MySQL hostname is '$hostname' (the cluster's own StatefulSet pod) -- NOT infra/compose.yaml's dev database." ;;
    *) fail "connected MySQL hostname is '$hostname', which does NOT look like the cluster's relaylab-mysql-* pod -- refusing to proceed, this may be pointed at infra/compose.yaml's dev database instead." ;;
  esac
}
