#!/usr/bin/env bash
#
# Create (or update) the fire-watch Secrets from .env.
#
#   ./scripts/create-secrets.sh            # apply to $NAMESPACE
#   ./scripts/create-secrets.sh --dry-run  # print the manifests, change nothing
#
# Idempotent: it applies, so re-running after a rotation just updates the values.
#
# Why a script and not the commented snippet in k8s/base/kustomization.yaml:
# that snippet regenerates a random password on every run, so the Dragonfly pods
# and the Sentinels would end up with DIFFERENT passwords and the cluster would
# come up broken. Reading one file makes the two provably the same value.
#
# WHAT IS AND IS NOT A SECRET HERE
#   AMQP_USER        a ConfigMap value. An account name is not a credential: it
#                    is worthless without the password. It lives in ConfigMap
#                    `fire-watch-rabbitmq-user` so it can be read with
#                    `kubectl get cm` instead of a secret round-trip.
#   AMQP_PASS        a Secret. The actual credential.
#   DRAGONFLY_PASSWORD, FW_INGEST_TOKEN   Secrets.
#
# Only `password` and `token` ever enter a Secret. If a value is not a
# credential, putting it in a Secret buys nothing and costs inspectability.
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "error: .env not found. Copy .env.example and fill it in." >&2
  exit 1
fi

# shellcheck disable=SC1091
set -a
. ./.env
set +a

: "${NAMESPACE:?NAMESPACE missing from .env}"
: "${AMQP_USER:?AMQP_USER missing from .env}"
: "${AMQP_PASS:?AMQP_PASS missing from .env}"
: "${DRAGONFLY_PASSWORD:?DRAGONFLY_PASSWORD missing from .env}"
: "${FW_INGEST_TOKEN:?FW_INGEST_TOKEN missing from .env}"

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
fi

# Always render to YAML first, then either print it or apply it. Rendering once
# keeps the dry-run output identical to what would be applied.
apply_secret() {
  local name="$1"; shift
  echo "==> secret/${name}" >&2
  local rendered
  rendered="$(kubectl -n "$NAMESPACE" create secret generic "$name" "$@" \
    --dry-run=client -o yaml)"
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '%s\n' "$rendered"
  else
    printf '%s\n' "$rendered" | kubectl apply -f - >&2
  fi
}

# ---------------------------------------------------------------------------
# Verify the ConfigMap's account name matches what the broker will create.
#
# RABBITMQ_DEFAULT_USER comes from the ConfigMap; the broker creates exactly one
# account from it. If .env's AMQP_USER disagrees, the broker gets account A and
# the clients authenticate as B — and the error surfaces as PLUGIN_AUTH_FAILURE
# in the worker logs, pointing at the password rather than the name.
# ---------------------------------------------------------------------------
CM_FILE=k8s/base/12-rabbitmq-user-config.yaml
cm_user="$(sed -n 's/^  AMQP_USER: *"\{0,1\}\([^"]*\)"\{0,1\}$/\1/p' "$CM_FILE")"
if [[ "$cm_user" != "$AMQP_USER" ]]; then
  echo >&2
  echo "ERROR: AMQP_USER mismatch." >&2
  echo "  .env                AMQP_USER=$AMQP_USER" >&2
  echo "  $CM_FILE  AMQP_USER=$cm_user" >&2
  echo >&2
  echo "The broker creates its only account from the ConfigMap value, so a" >&2
  echo "mismatch means every client authenticates as a non-existent user." >&2
  echo "Change $CM_FILE to match .env, then re-run." >&2
  exit 1
fi
echo "==> ConfigMap fire-watch-rabbitmq-user agrees: AMQP_USER=$cm_user" >&2

# Secrets are create-or-update, so the namespace must already exist. It is not
# in k8s/base on purpose: the manifests are meant to be deployable into an
# existing team namespace, not to own it.
#
# `password` only. AMQP_USER is NOT written here — it comes from ConfigMap
# fire-watch-rabbitmq-user (k8s/base/12-rabbitmq-user-config.yaml). Putting a
# non-credential in a Secret makes it harder to read for no benefit.
apply_secret fire-watch-rabbitmq \
  --from-literal=password="$AMQP_PASS"

apply_secret fire-watch-dragonfly \
  --from-literal=password="$DRAGONFLY_PASSWORD"

apply_secret fire-watch-ingress \
  --from-literal=token="$FW_INGEST_TOKEN"


echo
echo "Done. Next:"
echo "  kubectl apply -k k8s/overlays/lab/openshift     # or .../kubernetes"
echo
echo "Order matters on a FIRST install: Dragonfly reads --requirepass from the"
echo "Secret at start, and the Sentinels read DRAGONFLY_PASSWORD when they seed"
echo "their config. Create the Secrets before the workloads, which is what this"
echo "script does."
