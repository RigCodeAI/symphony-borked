#!/usr/bin/env python3
"""Load only protected coordinator credentials, then exec the pinned runtime."""
import json
import os
from pathlib import Path
import pwd
import re
import stat
import sys

COORDINATOR_DEFAULT_SECRET_ENVS = {"LINEAR_API_KEY", "LINEAR_API_TOKEN"}
COORDINATOR_ADDITIONAL_SECRET_PAIRS = (
    {"FACTORY_HIGH_CLIENT_SECRET", "FACTORY_HIGH_WEBHOOK_SECRET"},
    {"FACTORY_DAYBREAK_CLIENT_SECRET", "FACTORY_DAYBREAK_WEBHOOK_SECRET"},
)
COORDINATOR_SECRET_ENVS = COORDINATOR_DEFAULT_SECRET_ENVS | set().union(*COORDINATOR_ADDITIONAL_SECRET_PAIRS)
ALLOWED_ENV = COORDINATOR_SECRET_ENVS | {"OAUTH_TOKEN"}
LINEAR_SECRET_NAMES_ENV = "FACTORY_LINEAR_SECRET_NAMES_JSON"
LINEAR_WORKFLOW_ROOT = Path("/etc/factory/workflows")
PINNED_VERSION = re.compile(r"[1-9][0-9]*")
FINGERPRINT = re.compile(r"[0-9a-f]{64}")


def protected_path(path, mode=None, gid=None):
    for component in [*reversed(path.parents), path]:
        info = component.lstat()
        if stat.S_ISLNK(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError("unsafe coordinator configuration path")
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("unsafe coordinator configuration file")
    if mode is not None and (stat.S_IMODE(info.st_mode) != mode or info.st_gid != gid):
        raise ValueError("unsafe coordinator credential permissions")


def _coordinator_environment_names(names):
    if not isinstance(names, set) or not COORDINATOR_DEFAULT_SECRET_ENVS.issubset(names):
        raise ValueError("coordinator credentials are incomplete")
    if not names.issubset(COORDINATOR_SECRET_ENVS):
        raise ValueError("coordinator credentials are invalid")
    for pair in COORDINATOR_ADDITIONAL_SECRET_PAIRS:
        selected = names & pair
        if selected and selected != pair:
            raise ValueError("coordinator credential pair is incomplete")


def runtime(release, directory=Path("/run/factory/coordinator"), public_path=Path("/etc/factory/public.json")):
    revision = release.name
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("invalid coordinator release")
    path = directory / (revision + ".json")
    environment = {
        key: value for key, value in os.environ.items()
        if key not in ALLOWED_ENV and key != LINEAR_SECRET_NAMES_ENV
    }
    workflow = "pilot"
    expected_workflow = "pilot"
    expected_env = set()
    expected_versions = {}
    expected_fingerprints = {}
    expected_webhook = False
    requires_file = False
    active = None
    if public_path.exists() or public_path.is_symlink():
        protected_path(public_path)
        active = json.loads(public_path.read_text())
    pending_path = public_path.with_name("public.pending.json")
    if pending_path.exists() or pending_path.is_symlink():
        protected_path(pending_path)
        pending = json.loads(pending_path.read_text())
        keys = {"coordinator_workflow": "pilot", "coordinator_secret_env": {},
                "coordinator_secret_versions": {}, "coordinator_secret_fingerprints": {},
                "enable_linear_webhook": False}
        same_pins = active is not None and all(pending.get(key, default) == active.get(key, default) for key, default in keys.items())
        if pending.get("service_revision") == revision and (active is None or active.get("service_revision") != revision or same_pins):
            public_path = pending_path
    if public_path.exists() or public_path.is_symlink():
        protected_path(public_path)
        public = json.loads(public_path.read_text())
        expected_workflow = public.get("coordinator_workflow", "pilot")
        secret_env = public.get("coordinator_secret_env", {})
        if not isinstance(secret_env, dict) or any(
                not isinstance(name, str) or not isinstance(key, str) or not key
                for name, key in secret_env.items()):
            raise ValueError("coordinator runtime differs from active configuration")
        expected_env = set(secret_env)
        expected_versions = public.get("coordinator_secret_versions", {})
        expected_fingerprints = public.get("coordinator_secret_fingerprints", {})
        expected_webhook = public.get("enable_linear_webhook", False)
        requires_file = "coordinator_workflow" in public
        if public.get("service_revision") != revision or expected_workflow not in {"pilot", "linear"}:
            raise ValueError("coordinator runtime differs from active configuration")
        if expected_workflow == "linear":
            _coordinator_environment_names(expected_env)
            if (len(set(secret_env.values())) != len(secret_env)
                    or not isinstance(expected_versions, dict)
                    or not isinstance(expected_fingerprints, dict)
                    or set(expected_versions) != expected_env or set(expected_fingerprints) != expected_env
                    or any(not isinstance(version, str) or not PINNED_VERSION.fullmatch(version)
                           for version in expected_versions.values())
                    or any(not isinstance(fingerprint, str) or not FINGERPRINT.fullmatch(fingerprint)
                           for fingerprint in expected_fingerprints.values())):
                raise ValueError("coordinator runtime differs from active configuration")
        elif expected_env or expected_versions or expected_fingerprints:
            raise ValueError("pilot workflow cannot receive integration credentials")
    if path.exists() or path.is_symlink():
        protected_path(path, mode=0o640, gid=pwd.getpwnam("factory-coordinator").pw_gid)
        if path.stat().st_size > 65536:
            raise ValueError("invalid coordinator runtime configuration")
        value = json.loads(path.read_text())
        if (not isinstance(value, dict) or set(value) != {"service_revision", "workflow", "environment", "secret_versions", "secret_fingerprints", "enable_linear_webhook"}
                or value["service_revision"] != revision or value["workflow"] not in {"pilot", "linear"}
                or not isinstance(value["environment"], dict) or not set(value["environment"]).issubset(ALLOWED_ENV)
                or any(not isinstance(item, str) or not item or len(item.encode()) > 16384
                       or any(ord(char) < 32 or ord(char) == 127 for char in item)
                       for item in value["environment"].values())):
            raise ValueError("invalid coordinator runtime configuration")
        workflow = value["workflow"]
        if (workflow != expected_workflow or set(value["environment"]) != expected_env or value["secret_versions"] != expected_versions
                or value["secret_fingerprints"] != expected_fingerprints or value["enable_linear_webhook"] != expected_webhook):
            raise ValueError("coordinator runtime differs from active configuration")
        environment.update(value["environment"])
    elif requires_file or expected_workflow != "pilot" or expected_env:
        raise ValueError("coordinator runtime configuration is missing")
    if workflow == "linear":
        if ALLOWED_ENV.intersection(environment) != expected_env:
            raise ValueError("coordinator credentials are incomplete")
        workflow_path = LINEAR_WORKFLOW_ROOT / (revision + ".md")
        protected_path(workflow_path)
        # The Elixir Config reader checks this trusted pin against workflow references.
        environment[LINEAR_SECRET_NAMES_ENV] = json.dumps(sorted(expected_env))
    else:
        if ALLOWED_ENV.intersection(environment) or LINEAR_SECRET_NAMES_ENV in environment:
            raise ValueError("pilot workflow cannot receive integration credentials")
        workflow_path = release / "factory/deploy/PILOT-WORKFLOW.md"
    return workflow_path, environment


def main():
    if len(sys.argv) < 4:
        raise ValueError("invalid coordinator entrypoint arguments")
    release = Path(sys.argv[1])
    workflow, environment = runtime(release)
    os.execve(sys.argv[2], [*sys.argv[2:], str(workflow)], environment)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.stderr.write("factory coordinator runtime configuration failed\n")
        raise SystemExit(1)
