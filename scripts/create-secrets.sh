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
# that snippet regenerates a random password on every run, so the Valkey pods
# and the Sentinels would end up with DIFFERENT passwords and the cluster would
# come up broken. Reading one file makes the two provably the same value.
#
# WHAT IS A SECRET HERE
#   VALKEY_PASSWORD, FW_INGEST_TOKEN   Secrets.
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
: "${VALKEY_PASSWORD:?VALKEY_PASSWORD missing from .env}"
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

# Secrets are create-or-update, so the namespace must already exist. It is not
# in k8s/base on purpose: the manifests are meant to be deployable into an
# existing team namespace, not to own it.

apply_secret fire-watch-valkey \
  --from-literal=password="$VALKEY_PASSWORD"

apply_secret fire-watch-ingress \
  --from-literal=token="$FW_INGEST_TOKEN"


echo
echo "Done. Next:"
echo "  kubectl apply -k k8s/overlays/lab/openshift     # or .../kubernetes"
echo
echo "Order matters on a FIRST install: Valkey reads --requirepass from the"
echo "Secret at start, and the Sentinels read VALKEY_PASSWORD when they seed"
echo "their config. Create the Secrets before the workloads, which is what this"
echo "script does."
