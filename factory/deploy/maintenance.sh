#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
[[ "$EUID" -eq 0 ]] || { echo 'Maintenance requires root' >&2; exit 1; }
script_dir="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# Run the matching helper and uploader from one verified, root-owned release.
source "$script_dir/lib.sh"
factory_assert_root_tree "$script_dir"
case "${1:-}" in
  archive|backup)
    exec python3 -I "$script_dir/cloud_io.py" "$1"
    ;;
  health)
    if ! curl --fail --silent --max-time 15 http://127.0.0.1:8080/api/v1/state >/dev/null; then
      printf '%s FACTORY_EVENT service_unhealthy\n' "$(date -u +%FT%TZ)" >>/var/log/factory-events.log
      exit 1
    fi
    ;;
  *) echo 'Usage: maintenance.sh archive|backup|health' >&2; exit 2 ;;
esac
