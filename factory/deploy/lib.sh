#!/usr/bin/env bash

factory_fail() {
  printf 'factory: %s\n' "$*" >&2
  return 1
}

factory_release_revision() {
  local factory_local_release_dir="$1"
  python3 - "$factory_local_release_dir/RELEASE.json" <<'PY'
import json
import re
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as source:
        value = json.load(source).get("service_revision")
except (OSError, ValueError, AttributeError):
    raise SystemExit("invalid RELEASE.json")
if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}", value):
    raise SystemExit("RELEASE.json has no valid service_revision")
print(value)
PY
}

factory_release_sha256() {
  local factory_local_release_dir="$1" factory_local_sha_value
  [[ -f "$factory_local_release_dir/.verified-sha256" && ! -L "$factory_local_release_dir/.verified-sha256" ]] || {
    factory_fail "release SHA marker is missing"
    return 1
  }
  factory_local_sha_value="$(tr -d '[:space:]' <"$factory_local_release_dir/.verified-sha256")" || return 1
  [[ "$factory_local_sha_value" =~ ^[0-9a-f]{64}$ ]] || {
    factory_fail "release SHA marker is invalid"
    return 1
  }
  printf '%s\n' "$factory_local_sha_value"
}

factory_active_release() {
  local factory_local_root="$1" factory_local_current factory_local_releases
  local factory_local_resolved factory_local_revision
  factory_local_current="$factory_local_root/current"
  factory_local_releases="$(realpath -e -- "$factory_local_root/releases")" || {
    factory_fail "release directory is missing"
    return 1
  }
  [[ -L "$factory_local_current" ]] || {
    factory_fail "active release symlink is missing"
    return 1
  }
  factory_local_resolved="$(realpath -e -- "$factory_local_current")" || {
    factory_fail "active release symlink is broken"
    return 1
  }
  [[ "$factory_local_resolved" == "$factory_local_releases/"* && -d "$factory_local_resolved" ]] || {
    factory_fail "active release is outside the releases directory"
    return 1
  }
  factory_local_revision="$(factory_release_revision "$factory_local_resolved")" || return 1
  [[ "$(basename -- "$factory_local_resolved")" == "$factory_local_revision" ]] || {
    factory_fail "active release directory does not match its revision"
    return 1
  }
  printf '%s\n' "$factory_local_resolved"
}

factory_resolve_script() {
  local factory_local_source="$1" factory_local_dir factory_local_target
  while [[ -L "$factory_local_source" ]]; do
    factory_local_dir="$(cd -P -- "$(dirname -- "$factory_local_source")" >/dev/null 2>&1 && pwd)" || return 1
    factory_local_target="$(readlink -- "$factory_local_source")" || return 1
    if [[ "$factory_local_target" == /* ]]; then
      factory_local_source="$factory_local_target"
    else
      factory_local_source="$factory_local_dir/$factory_local_target"
    fi
  done
  realpath -e -- "$factory_local_source"
}

factory_load_runtime_env() {
  local factory_local_release_dir="$1" factory_local_role="$2"
  local factory_local_revision factory_local_sha factory_local_node_bin factory_local_codex_bin
  case "$factory_local_role" in
    coordinator|worker) ;;
    *) factory_fail "unknown runtime role"; return 1 ;;
  esac
  factory_local_revision="$(factory_release_revision "$factory_local_release_dir")" || return 1
  factory_local_sha="$(factory_release_sha256 "$factory_local_release_dir")" || return 1
  [[ "$(basename -- "$factory_local_release_dir")" == "$factory_local_revision" ]] || {
    factory_fail "release directory does not match its revision"
    return 1
  }

  export FACTORY_ROLE="$factory_local_role"
  export FACTORY_RELEASE_DIR="$factory_local_release_dir"
  export FACTORY_SERVICE_REVISION="$factory_local_revision"
  export FACTORY_RELEASE_SHA256="$factory_local_sha"
  # These roots are deployment constants. A worker request cannot redirect the
  # pinned runner to another source tree by supplying shell environment values.
  export FACTORY_ROOT="/opt/factory"
  export FACTORY_DATA_ROOT="/srv/factory"
  export MISE_DATA_DIR="$FACTORY_DATA_ROOT/mise"
  export MIX_HOME="$FACTORY_DATA_ROOT/homes/factory-$factory_local_role/.mix"
  export MISE_CACHE_DIR="$FACTORY_DATA_ROOT/homes/factory-$factory_local_role/.cache/mise"
  export MISE_TRUSTED_CONFIG_PATHS="$factory_local_release_dir/elixir"
  # Bootstrap never executes the legacy service-writable tools tree. Runtime
  # binaries come from a new root-owned namespace; Cargo's writable cache is
  # separate and is never used by a root process.
  export CARGO_HOME="$FACTORY_DATA_ROOT/homes/factory-$factory_local_role/.cargo"
  export RUSTUP_HOME="$FACTORY_DATA_ROOT/bootstrap-tools-v1/rustup"
  export RIG_TOOLS_DIR="$FACTORY_DATA_ROOT/bootstrap-tools-v1/rig-tools"
  export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-3}"
  export TMPDIR="$FACTORY_DATA_ROOT/tmp"
  export MIX_BUILD_PATH="$FACTORY_DATA_ROOT/build/$factory_local_revision"
  export MIX_DEPS_PATH="$FACTORY_DATA_ROOT/build/$factory_local_revision/deps"

  factory_local_node_bin="$RIG_TOOLS_DIR/node-v24.19.0-linux-x64/bin"
  if [[ -x "$RIG_TOOLS_DIR/node-v24.19.0-linux-arm64/bin/node" ]]; then
    factory_local_node_bin="$RIG_TOOLS_DIR/node-v24.19.0-linux-arm64/bin"
  fi
  factory_local_codex_bin="$RIG_TOOLS_DIR/codex/bin"
  export PATH="$FACTORY_DATA_ROOT/bootstrap-tools-v1/cargo/bin:$factory_local_node_bin:$factory_local_codex_bin:${PATH:-/usr/local/bin:/usr/bin:/bin}"

  [[ -d "$FACTORY_DATA_ROOT/homes/factory-$factory_local_role" ]] || { factory_fail "service home is missing"; return 1; }
  [[ -d "$TMPDIR" ]] || { factory_fail "runtime temporary directory is missing"; return 1; }
  mkdir -p -- "$MISE_CACHE_DIR"
  return 0
}

factory_mix() {
  [[ "$EUID" -ne 0 ]] || { factory_fail "Mix requires the service identity"; return 1; }
  # mise's Elixir backend supplies its own MIX_HOME and MIX_ARCHIVES. Apply
  # role caches after runtime selection so those writes stay unprivileged.
  /usr/local/bin/mise exec -- /usr/bin/env \
    MIX_HOME="${MIX_HOME:?runtime environment is required}" \
    MIX_ARCHIVES="$MIX_HOME/archives" MIX_ESCRIPTS="$MIX_HOME/escripts" \
    mix "$@"
}

factory_atomic_json() {
  local factory_local_target="$1" factory_local_content="$2" factory_local_temp
  factory_local_temp="${factory_local_target}.tmp.$$"
  (umask 077; printf '%s\n' "$factory_local_content" >"$factory_local_temp") || return 1
  mv -f -- "$factory_local_temp" "$factory_local_target"
}

# Startup is standalone before a release exists; release entrypoints share this guard.
factory_assert_root_tree() {
  local factory_tree_path="$1" factory_tree_root factory_tree_entry
  local factory_tree_owner factory_tree_mode factory_tree_target
  [[ -d "$factory_tree_path" && ! -L "$factory_tree_path" ]] || {
    echo 'Protected runtime path must be a real directory' >&2
    return 1
  }
  factory_tree_root="$(realpath -e -- "$factory_tree_path")" || return 1
  while IFS= read -r -d '' factory_tree_entry; do
    factory_tree_owner="$(stat -c %u -- "$factory_tree_entry")" || return 1
    [[ "$factory_tree_owner" = 0 ]] || {
      echo 'Protected runtime tree contains a non-root-owned path' >&2
      return 1
    }
    if [[ -L "$factory_tree_entry" ]]; then
      factory_tree_target="$(realpath -e -- "$factory_tree_entry")" || {
        echo 'Protected runtime tree contains a broken symlink' >&2
        return 1
      }
      case "$factory_tree_target" in
        "$factory_tree_root"|"$factory_tree_root"/*|/usr/local/bin/mise) ;;
        *) echo 'Protected runtime tree contains an escaping symlink' >&2; return 1 ;;
      esac
      continue
    fi
    [[ -d "$factory_tree_entry" || -f "$factory_tree_entry" ]] || {
      echo 'Protected runtime tree contains a special file' >&2
      return 1
    }
    factory_tree_mode="$(stat -c %a -- "$factory_tree_entry")" || return 1
    [[ "$factory_tree_mode" =~ ^[0-7]{3,4}$ ]] && \
      (( (8#$factory_tree_mode & 0022) == 0 )) || {
        echo 'Protected runtime tree is group- or world-writable' >&2
        return 1
      }
  done < <(find -P "$factory_tree_root" -print0)
}
