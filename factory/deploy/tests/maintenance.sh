#!/usr/bin/env bash
# Exercise the actual entrypoint in a disposable root Linux container.
set -Eeuo pipefail
umask 077
source_root="${FACTORY_TEST_SOURCE:-/workspace/factory/deploy}"
[[ "$EUID" -eq 0 && -f /.dockerenv ]] || exit 2
revision=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
release="/srv/factory/releases/$revision"
[[ ! -e /opt/factory && ! -e /srv/factory ]] || {
  echo 'Use a clean disposable container' >&2
  exit 2
}
trap 'rm -rf /opt/factory /srv/factory' EXIT
mkdir -p "$release/factory/deploy" /opt/factory
chmod 0755 /srv /srv/factory /srv/factory/releases "$release" "$release/factory" "$release/factory/deploy" /opt/factory
cp "$source_root/lib.sh" "$source_root/maintenance.sh" "$release/factory/deploy/"
printf '{"service_revision":"%s"}\n' "$revision" >"$release/RELEASE.json"
printf '%064d\n' 0 >"$release/.verified-sha256"
cat >"$release/factory/deploy/cloud_io.py" <<'PY'
import sys
assert sys.flags.isolated == 1
assert sys.argv[1] in ['archive','backup']
print('MATCHING_ISOLATED_HELPER '+sys.argv[1])
PY
chmod -R a+rX "$release"
ln -s /srv/factory/releases /opt/factory/releases
ln -s "$release" /opt/factory/current
for command in archive backup; do
 output="$($release/factory/deploy/maintenance.sh "$command")"
 [[ "$output" == "MATCHING_ISOLATED_HELPER $command" ]]
done
chmod 0664 "$release/factory/deploy/cloud_io.py"
if "$release/factory/deploy/maintenance.sh" archive >/tmp/maintenance-writable.out 2>&1; then
 echo 'Maintenance accepted writable helper' >&2; exit 1
fi
! grep -q MATCHING_ISOLATED_HELPER /tmp/maintenance-writable.out
chmod 0644 "$release/factory/deploy/cloud_io.py"
chown 65534:65534 "$release/factory/deploy/cloud_io.py"
if "$release/factory/deploy/maintenance.sh" archive >/tmp/maintenance-owner.out 2>&1; then
 echo 'Maintenance accepted service-owned helper' >&2; exit 1
fi
! grep -q MATCHING_ISOLATED_HELPER /tmp/maintenance-owner.out
chown 0:0 "$release/factory/deploy/cloud_io.py"
mkdir -p /srv/factory/releases/ffffffffffffffffffffffffffffffffffffffff
printf '{"service_revision":"%s"}\n' ffffffffffffffffffffffffffffffffffffffff >/srv/factory/releases/ffffffffffffffffffffffffffffffffffffffff/RELEASE.json
ln -s /srv/factory/releases/ffffffffffffffffffffffffffffffffffffffff /opt/factory/.next
mv -Tf /opt/factory/.next /opt/factory/current
if "$release/factory/deploy/maintenance.sh" archive >/tmp/maintenance-old.out 2>&1; then
 echo 'Maintenance accepted inactive release' >&2; exit 1
fi
! grep -q MATCHING_ISOLATED_HELPER /tmp/maintenance-old.out
printf 'PASS: active root-owned release invokes matching isolated archive/backup helper; unsafe and inactive helpers reject\n'
