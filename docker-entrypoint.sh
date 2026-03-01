#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  docker-entrypoint.sh source <file_path> [source args...]
  docker-entrypoint.sh sink [sink args...]

Optional environment variables:
  TCP_SOURCE_FILE  Default source file path if omitted in CLI args.
EOF
}

COMPONENT="${1:-}"

if [[ -z "${COMPONENT}" ]]; then
  echo "ERROR: Missing component."
  usage
  exit 1
fi

case "${COMPONENT}" in
  source)
    shift
    if [[ "$#" -eq 0 && -n "${TCP_SOURCE_FILE:-}" ]]; then
      set -- "${TCP_SOURCE_FILE}"
    fi

    if [[ "$#" -eq 0 ]]; then
      echo "ERROR: source requires a file path as first argument (or TCP_SOURCE_FILE env var)."
      usage
      exit 1
    fi

    SOURCE_FILE="$1"
    if [[ ! -f "${SOURCE_FILE}" ]]; then
      echo "ERROR: Source input file not found: ${SOURCE_FILE}"
      echo "Mount the file (for example to /data) and pass that container path."
      exit 1
    fi

    exec tcp_source "$@"
    ;;
  sink)
    shift
    exec tcp_sink "$@"
    ;;
  *)
    echo "ERROR: First argument must be 'source' or 'sink', got '${COMPONENT}'."
    usage
    exit 1
    ;;
esac
