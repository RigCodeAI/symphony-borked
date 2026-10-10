#!/usr/bin/env python3
"""Small root-only GCP transport for bootstrap, backups and redacted pilot evidence.

Credential values and access tokens never go through Terraform or stdout. Agent
users cannot invoke this transport or obtain the instance metadata identity.
"""
import argparse
import base64
import binascii
from contextlib import closing
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import pwd
import re
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

CONFIG = Path("/etc/factory/config.json")
PUBLIC_CONFIG = Path("/etc/factory/public.json")
EVENTS = Path("/var/log/factory-events.log")
DATA_ROOT = Path("/srv/factory")
MODEL_AUTH_PATH = Path("/srv/factory/homes/factory-worker/.codex/auth.json")
MODEL_AUTH_SOURCE = DATA_ROOT / "model-auth.source.json"
LEGACY_MODEL_AUTH_PATH = Path("/run/factory/model-auth.json")
WORKER_HOST_KEY_DIR = DATA_ROOT / "ssh-host-keys"
WORKER_HOST_PRIVATE_KEY = WORKER_HOST_KEY_DIR / "ssh_host_ed25519_key"
WORKER_HOST_PUBLIC_KEY = WORKER_HOST_KEY_DIR / "ssh_host_ed25519_key.pub"
WORKER_HOST_KEY_MARKER = DATA_ROOT / "worker-hostkey.json"
MAX_MODEL_AUTH_BYTES = 1024 * 1024
TOKEN_PATTERN = re.compile(r"(?:sk-|gh[pousr]_|github_pat_)[A-Za-z0-9_-]+|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
COORDINATOR_DEFAULT_SECRET_ENVS = {"LINEAR_API_KEY", "LINEAR_API_TOKEN"}
COORDINATOR_ADDITIONAL_SECRET_PAIRS = (
    {"FACTORY_HIGH_CLIENT_SECRET", "FACTORY_HIGH_WEBHOOK_SECRET"},
    {"FACTORY_DAYBREAK_CLIENT_SECRET", "FACTORY_DAYBREAK_WEBHOOK_SECRET"},
)
COORDINATOR_SECRET_ENVS = COORDINATOR_DEFAULT_SECRET_ENVS | set().union(*COORDINATOR_ADDITIONAL_SECRET_PAIRS)


def event(name):
    with EVENTS.open("a") as handle:
        handle.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} FACTORY_EVENT {name}\n")


def request(url, method="GET", data=None, headers=None):
    # Do not include response bodies, URLs or Authorization in error messages.
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if attempt < 5 and exc.code in (401, 403, 404, 429, 500, 502, 503, 504):
                time.sleep(min(2 ** attempt, 8))
                continue
            raise RuntimeError(f"cloud request failed: HTTP {exc.code}") from None
        except urllib.error.URLError:
            if attempt < 5:
                time.sleep(min(2 ** attempt, 8))
                continue
            raise RuntimeError("cloud request failed: transport unavailable") from None


class Cloud:
    def __init__(self, config):
        self.config = config

    def headers(self):
        reply = json.loads(request(
            "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"}))
        return {"Authorization": "Bearer " + reply["access_token"]}

    def object(self, bucket, name):
        return request("https://storage.googleapis.com/storage/v1/b/" +
                       urllib.parse.quote(bucket, safe="") + "/o/" +
                       urllib.parse.quote(name, safe="") + "?alt=media", headers=self.headers())

    def put(self, bucket, name, payload):
        url = "https://storage.googleapis.com/upload/storage/v1/b/" + urllib.parse.quote(bucket, safe="")
        url += "/o?uploadType=media&ifGenerationMatch=0&name=" + urllib.parse.quote(name, safe="")
        # Immutable names: an existing object never gets overwritten. Compare its
        # checksum on retries, so interrupted archival can safely finish.
        try:
            return request(url, "POST", payload,
                           {**self.headers(), "Content-Type": "application/octet-stream"})
        except RuntimeError as exc:
            if str(exc) != "cloud request failed: HTTP 412":
                raise
            if self.object(bucket, name) != payload:
                raise RuntimeError("existing object checksum differs; refusing overwrite") from None
            return b""

    def secret(self, key):
        reference = self.config["secret_ids"][key]
        version = self.config["secret_versions"][key]
        prefix = "projects/" + self.config["project_id"] + "/secrets/"
        if not reference.startswith(prefix) or not re.fullmatch(r"[1-9][0-9]*", str(version)):
            raise ValueError("secret reference must use this project and a pinned numeric version")
        data = json.loads(request("https://secretmanager.googleapis.com/v1/" + reference +
                                 "/versions/" + str(version) + ":access", headers=self.headers()))
        return base64.b64decode(data["payload"]["data"], validate=True)


def _read_regular_file(path, expected_uid=None, expected_mode=None, limit=MAX_MODEL_AUTH_BYTES):
    """Read a bounded regular file without following its final path component."""
    flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(path, flags)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > limit:
            raise ValueError("model auth file must be a single-link regular file within the size limit")
        if expected_uid is not None and info.st_uid != expected_uid:
            raise ValueError("model auth file has an unexpected owner")
        if expected_mode is not None and stat.S_IMODE(info.st_mode) != expected_mode:
            raise ValueError("model auth file has unexpected permissions")
        data = handle.read(limit + 1)
        if len(data) > limit:
            raise ValueError("model auth file exceeds the size limit")
        return data


def _validate_chatgpt_auth(payload):
    try:
        auth = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("model auth is not valid JSON") from None
    tokens = auth.get("tokens") if isinstance(auth, dict) else None
    if (not isinstance(auth, dict) or auth.get("auth_mode") != "chatgpt" or
            not isinstance(tokens, dict) or
            not isinstance(tokens.get("access_token"), str) or not tokens["access_token"].strip() or
            not isinstance(tokens.get("refresh_token"), str) or not tokens["refresh_token"].strip()):
        raise ValueError("model auth must contain ChatGPT access and refresh tokens")
    return auth


def _assert_no_symlink_components(path):
    current = Path(path.anchor)
    for part in Path(path).parts[1:]:
        current = current / part
        try:
            info = current.lstat()
        except FileNotFoundError:
            raise ValueError("model auth path has a missing directory") from None
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ValueError("model auth path contains an unsafe directory")


def _check_directory(path, owner_uid, private=False):
    info = Path(path).lstat()
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != owner_uid:
        raise ValueError("model auth directory has an unexpected owner or type")
    if mode & (0o077 if private else 0o022):
        raise ValueError("model auth directory has unsafe permissions")


def _check_model_auth_paths(auth_path, marker_path, worker_uid, root_uid, strict_paths):
    if strict_paths:
        _assert_no_symlink_components(auth_path.parent)
        _assert_no_symlink_components(marker_path.parent)
        expected_auth = MODEL_AUTH_PATH
        expected_marker = MODEL_AUTH_SOURCE
        if auth_path != expected_auth or marker_path != expected_marker:
            raise ValueError("model auth paths are fixed by deployment policy")
        _check_directory(DATA_ROOT, root_uid)
        _check_directory(auth_path.parent.parent, worker_uid)
        _check_directory(auth_path.parent, worker_uid, private=True)
        _check_directory(marker_path.parent, root_uid)
    else:
        _check_directory(auth_path.parent, worker_uid, private=True)
        _check_directory(marker_path.parent, root_uid)


def _read_source_marker(marker_path, root_uid):
    try:
        info = marker_path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError("model auth source marker must not be a symlink")
    payload = _read_regular_file(marker_path, expected_uid=root_uid, expected_mode=0o600, limit=4096)
    try:
        marker = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("model auth source marker is invalid") from None
    if not isinstance(marker, dict):
        raise ValueError("model auth source marker is invalid")
    return marker


def _atomic_private_write(path, payload, owner_uid, owner_gid, mode=0o600):
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    temporary = Path(temporary)
    try:
        os.fchmod(fd, mode)
        os.fchown(fd, owner_uid, owner_gid)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _model_auth_source(config):
    reference = config.get("secret_ids", {}).get("model_auth")
    version = str(config.get("secret_versions", {}).get("model_auth", ""))
    prefix = "projects/" + str(config.get("project_id", "")) + "/secrets/"
    if (not isinstance(reference, str) or not reference.startswith(prefix) or
            not re.fullmatch(r"[1-9][0-9]*", version)):
        raise ValueError("model auth must use a project-scoped secret and pinned numeric version")
    return {"secret_id": reference, "secret_version": version}


def _auth_file_metadata(path, worker_uid):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return "missing"
    if stat.S_ISLNK(info.st_mode):
        return "symlink"
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != worker_uid or
            stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1 or
            info.st_size > MAX_MODEL_AUTH_BYTES):
        raise ValueError("existing model auth file has unsafe type, owner, permissions, or size")
    return "regular"


def ensure_model_auth(cloud, auth_path=MODEL_AUTH_PATH, marker_path=MODEL_AUTH_SOURCE,
                      legacy_path=LEGACY_MODEL_AUTH_PATH, worker_uid=None, worker_gid=None,
                      root_uid=None, root_gid=None, strict_paths=True):
    """Seed durable worker auth once per pinned secret version, preserving refreshes."""
    if cloud.config.get("role") != "worker":
        raise ValueError("model auth may be installed only on a worker")
    source = _model_auth_source(cloud.config)
    account = pwd.getpwnam("factory-worker") if worker_uid is None or worker_gid is None else None
    worker_uid = account.pw_uid if worker_uid is None else worker_uid
    worker_gid = account.pw_gid if worker_gid is None else worker_gid
    root_uid = os.geteuid() if root_uid is None else root_uid
    root_gid = pwd.getpwuid(root_uid).pw_gid if root_gid is None else root_gid
    auth_path, marker_path, legacy_path = Path(auth_path), Path(marker_path), Path(legacy_path)
    _check_model_auth_paths(auth_path, marker_path, worker_uid, root_uid, strict_paths)
    previous_source = _read_source_marker(marker_path, root_uid)
    status = _auth_file_metadata(auth_path, worker_uid)

    if status == "regular" and previous_source == source:
        _validate_chatgpt_auth(_read_regular_file(auth_path, worker_uid, 0o600))
        return "preserved"

    if status == "symlink":
        try:
            target = os.readlink(auth_path)
        except OSError:
            raise ValueError("legacy model auth symlink cannot be inspected") from None
        if target != str(legacy_path):
            raise ValueError("model auth symlink target is not the known legacy path")
        if previous_source == source or previous_source is None:
            if strict_paths:
                _assert_no_symlink_components(legacy_path.parent)
            else:
                _check_directory(legacy_path.parent, root_uid)
            legacy_payload = _read_regular_file(legacy_path, worker_uid, 0o600)
            _validate_chatgpt_auth(legacy_payload)
            _atomic_private_write(auth_path, legacy_payload, worker_uid, worker_gid)
            if previous_source is None:
                marker = json.dumps(source, sort_keys=True).encode() + b"\n"
                _atomic_private_write(marker_path, marker, root_uid, root_gid)
            return "migrated"

    # Missing auth or an intentional source-version change seeds from the configured
    # Secret Manager version. Unknown existing files without a source marker are
    # replaced from that pinned source; known same-version files are preserved above.
    secret_payload = cloud.secret("model_auth")
    _validate_chatgpt_auth(secret_payload)
    _atomic_private_write(auth_path, secret_payload, worker_uid, worker_gid)
    marker = json.dumps(source, sort_keys=True).encode() + b"\n"
    _atomic_private_write(marker_path, marker, root_uid, root_gid)
    return "seeded"


def _host_key_directory(path, owner_uid, strict_paths):
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("worker host-key path must be absolute")
    parent = path.parent
    if strict_paths:
        _assert_no_symlink_components(parent)
    _check_directory(parent, owner_uid)
    try:
        info = path.lstat()
    except FileNotFoundError:
        try:
            path.mkdir(mode=0o700)
            os.chown(path, owner_uid, pwd.getpwuid(owner_uid).pw_gid)
            os.chmod(path, 0o700)
        except FileExistsError:
            pass
        info = path.lstat()
    if (stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode) or
            info.st_uid != owner_uid or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError("worker host-key directory has an unsafe type, owner, or mode")
    if strict_paths:
        _assert_no_symlink_components(path)
    return path


def _host_key_file_state(path, owner_uid, mode):
    try:
        info = Path(path).lstat()
    except FileNotFoundError:
        return False
    if (stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or
            info.st_uid != owner_uid or stat.S_IMODE(info.st_mode) != mode or info.st_nlink != 1):
        raise ValueError("worker host-key file has an unsafe type, owner, mode, or link count")
    return True


def _host_key_public_line(private_path):
    try:
        result = subprocess.run(
            ["ssh-keygen", "-y", "-f", str(private_path)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        raise ValueError("retained worker host private key cannot be validated") from None
    parts = result.stdout.decode("ascii", errors="strict").strip().split()
    if len(parts) < 2 or parts[0] != "ssh-ed25519":
        raise ValueError("retained worker host key must be ed25519")
    try:
        raw_key = base64.b64decode(parts[1], validate=True)
    except (ValueError, binascii.Error):
        raise ValueError("retained worker public key is invalid") from None
    if len(raw_key) < 32:
        raise ValueError("retained worker public key is invalid")
    return f"ssh-ed25519 {parts[1]}\n"


def _write_host_key_file(path, payload, mode, owner_uid, replace):
    path = Path(path)
    owner_gid = pwd.getpwuid(owner_uid).pw_gid
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    temporary = Path(temporary)
    try:
        os.fchmod(fd, mode)
        os.fchown(fd, owner_uid, owner_gid)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if replace:
            os.replace(temporary, path)
        else:
            os.link(temporary, path, follow_symlinks=False)
            temporary.unlink()
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _host_key_marker_digest(marker_path, owner_uid):
    marker_path = Path(marker_path)
    try:
        info = marker_path.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(info.st_mode):
        raise ValueError("worker host-key marker must not be a symlink")
    data = _read_regular_file(marker_path, expected_uid=owner_uid, expected_mode=0o600, limit=4096)
    try:
        marker = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("worker host-key marker is invalid") from None
    digest = marker.get("sha256") if isinstance(marker, dict) and marker.get("version") == 1 else None
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("worker host-key marker is invalid")
    return digest


def ensure_worker_host_key(host_key_dir=WORKER_HOST_KEY_DIR,
                           marker_path=WORKER_HOST_KEY_MARKER,
                           owner_uid=0, strict_paths=True):
    """Create one durable worker host identity, then refuse silent replacement."""
    host_key_dir = Path(host_key_dir)
    marker_path = Path(marker_path)
    if marker_path.parent != host_key_dir.parent:
        raise ValueError("worker host-key marker must be beside its key directory")
    directory = _host_key_directory(host_key_dir, owner_uid, strict_paths)
    _check_directory(marker_path.parent, owner_uid)
    if strict_paths:
        _assert_no_symlink_components(marker_path.parent)

    lock_path = directory / ".worker-hostkey.lock"
    lock_base_flags = os.O_RDWR | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        lock_fd = os.open(lock_path, lock_base_flags | os.O_CREAT | os.O_EXCL, 0o600)
        new_lock = True
    except FileExistsError:
        lock_fd = os.open(lock_path, lock_base_flags)
        new_lock = False
    try:
        if new_lock:
            os.fchown(lock_fd, owner_uid, pwd.getpwuid(owner_uid).pw_gid)
        lock_info = os.fstat(lock_fd)
        if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != owner_uid or
                stat.S_IMODE(lock_info.st_mode) != 0o600 or lock_info.st_nlink != 1):
            raise ValueError("worker host-key lock has an unsafe type, owner, mode, or link count")
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        private_path = directory / "ssh_host_ed25519_key"
        public_path = directory / "ssh_host_ed25519_key.pub"
        has_private = _host_key_file_state(private_path, owner_uid, 0o600)
        has_public = _host_key_file_state(public_path, owner_uid, 0o644)
        marker_digest = _host_key_marker_digest(marker_path, owner_uid)

        if not has_private:
            if has_public or marker_digest is not None:
                raise ValueError("retained worker host private key is missing; operator repair required")
            with tempfile.TemporaryDirectory(prefix=".worker-hostkey-", dir=directory) as temporary:
                generated_private = Path(temporary) / "ssh_host_ed25519_key"
                try:
                    subprocess.run(
                        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "factory-worker", "-f", str(generated_private)],
                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True,
                    )
                except (OSError, subprocess.CalledProcessError):
                    raise RuntimeError("could not create the durable worker host key") from None
                private_data = _read_regular_file(generated_private, expected_uid=owner_uid, expected_mode=0o600)
                public_line = _host_key_public_line(generated_private)
                _write_host_key_file(private_path, private_data, 0o600, owner_uid, replace=False)
                _write_host_key_file(public_path, public_line.encode(), 0o644, owner_uid, replace=False)
        else:
            _read_regular_file(private_path, expected_uid=owner_uid, expected_mode=0o600)
            public_line = _host_key_public_line(private_path)
            if has_public:
                current_public = _read_regular_file(public_path, expected_uid=owner_uid, expected_mode=0o644)
                current_parts = current_public.decode("ascii", errors="strict").strip().split()
                if len(current_parts) < 2 or current_parts[:2] != public_line.strip().split():
                    raise ValueError("retained worker public key does not match its private key")
                if current_public != public_line.encode():
                    _write_host_key_file(public_path, public_line.encode(), 0o644, owner_uid, replace=True)
            else:
                _write_host_key_file(public_path, public_line.encode(), 0o644, owner_uid, replace=False)

        digest = hashlib.sha256(public_line.encode()).hexdigest()
        if marker_digest is not None and marker_digest != digest:
            raise ValueError("retained worker host key differs from its durable marker")
        if marker_digest is None:
            marker = json.dumps({"version": 1, "sha256": digest}, sort_keys=True).encode() + b"\n"
            _write_host_key_file(marker_path, marker, 0o600, owner_uid, replace=False)
        return public_line.strip()
    finally:
        os.close(lock_fd)


def safe_unpack(payload, destination):
    destination = Path(destination)
    if destination.exists():
        raise ValueError("release destination already exists")
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            parts = Path(member.name).parts
            if (member.name.startswith("/") or ".." in parts or not parts or
                    not (member.isfile() or member.isdir())):
                raise ValueError("release archive contains unsafe paths or file types")
        destination.mkdir(parents=True, mode=0o755)
        destination.chmod(0o755)
        try:
            for member in members:
                target = destination / member.name
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True, mode=0o755)
                    target.chmod(0o755)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                    with archive.extractfile(member) as source, target.open("xb") as output:
                        output.write(source.read())
                    target.chmod(0o755 if member.mode & 0o111 else 0o644)
            for directory, _subdirs, _files in os.walk(destination):
                os.chmod(directory, 0o755)
        except Exception:
            # Preserve the rejected candidate for inspection; it is never activated.
            raise


def pack(repository, output):
    repo = Path(repository).resolve()
    if subprocess.check_output(["git", "status", "--porcelain"], cwd=repo).strip():
        raise ValueError("commit deployment source before packaging")
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    raw = subprocess.check_output(["git", "archive", "--format=tar", revision], cwd=repo)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as source, tarfile.open(fileobj=buffer, mode="w:") as dest:
        for member in source.getmembers():
            if not (member.isfile() or member.isdir()):
                raise ValueError("release source must contain regular files/directories only")
            dest.addfile(member, source.extractfile(member) if member.isfile() else None)
        data = json.dumps({"service_revision": revision}, sort_keys=True).encode()
        marker = tarfile.TarInfo("RELEASE.json")
        marker.size, marker.mode, marker.mtime = len(data), 0o644, 0
        dest.addfile(marker, io.BytesIO(data))
    payload = gzip.compress(buffer.getvalue(), mtime=0)
    with Path(output).open("xb") as handle:
        handle.write(payload)
    print(json.dumps({"service_revision": revision, "release_sha256": hashlib.sha256(payload).hexdigest(),
                      "release_object": f"releases/{revision}.tar.gz", "path": str(Path(output).resolve())}))


def credential_values():
    values = []
    for path in (MODEL_AUTH_PATH, LEGACY_MODEL_AUTH_PATH):
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode):
            # The old deployment linked the retained home into /run. Read the
            # known target separately below, never follow the home symlink.
            if path == MODEL_AUTH_PATH and os.readlink(path) == str(LEGACY_MODEL_AUTH_PATH):
                continue
            raise ValueError("model auth path must be a regular file for redaction")
        payload = _read_regular_file(path, expected_mode=0o600)
        data = _validate_chatgpt_auth(payload)
        def walk(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if isinstance(item, str) and re.search(r"token|secret|password|key", key, re.I) and len(item) >= 8:
                        values.append(item)
                    else:
                        walk(item)
            elif isinstance(value, list):
                for item in value:
                    walk(item)
        walk(data)
    return values


def redact(payload, values):
    text = payload.decode("utf-8", errors="replace")
    for value in values:
        text = text.replace(value, "[REDACTED]")
    return TOKEN_PATTERN.sub("[REDACTED]", text).encode()


def regular_read(directory_fd, name):
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory_fd)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 10 * 1024 * 1024:
            raise ValueError("pilot evidence must be a regular file at most 10 MiB")
        data = handle.read(10 * 1024 * 1024 + 1)
        if len(data) > 10 * 1024 * 1024:
            raise ValueError("pilot evidence exceeds 10 MiB per artifact")
        return data


def archive_runs(cloud):
    root = DATA_ROOT / "runs"
    for run in sorted(root.iterdir()):
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", run.name) or not run.is_dir() or run.is_symlink():
            raise ValueError("invalid run directory")
        if not (run / "finished.json").is_file():
            continue
        fd = os.open(run, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            try:
                marker = os.stat(".archived", dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                marker = None
            if marker:
                if marker.st_uid != 0 or not stat.S_ISREG(marker.st_mode):
                    raise ValueError("archive completion marker must be root-owned and regular")
                continue
            manifest = {"version": 1, "run_id": run.name, "artifacts": {}}
            values = credential_values()
            names = ("report.json", "mix.log", "candidate.diff", "finished.json", "revision.json")
            if cloud.config.get("role") == "coordinator":
                names += ("submission.json",)
            for name in names:
                data = redact(regular_read(fd, name), values)
                digest = hashlib.sha256(data).hexdigest()
                cloud.put(cloud.config["archive_bucket"], f"runs/{run.name}/{name}", data)
                manifest["artifacts"][name] = {"sha256": digest, "bytes": len(data)}
            data = json.dumps(manifest, sort_keys=True, indent=2).encode()
            cloud.put(cloud.config["archive_bucket"], f"runs/{run.name}/manifest.json", data)
            marker_fd = os.open(".archived", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                0o600, dir_fd=fd)
            with os.fdopen(marker_fd, "w") as handle:
                handle.write(hashlib.sha256(data).hexdigest() + "\n")
        finally:
            os.close(fd)


def backup(cloud):
    # DEV-232 owns the runtime database. Back up any configured *.sqlite files
    # with SQLite's online backup API, never by copying live WAL files.
    bucket = cloud.config["backup_bucket"]
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    active = DATA_ROOT / "active.json"
    revision = json.loads(active.read_text())["service_revision"] if active.exists() else cloud.config["service_revision"]
    manifest = {"version": 1, "service_revision": revision, "databases": []}
    state = DATA_ROOT / "state"
    with tempfile.TemporaryDirectory(prefix="factory-backup-") as directory:
        for path in sorted(state.glob("*.sqlite")):
            if path.is_symlink():
                raise ValueError("backup database must not be a symlink")
            target = Path(directory) / path.name
            with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)) as source, closing(sqlite3.connect(target)) as dest:
                source.backup(dest)
                if dest.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise RuntimeError("SQLite backup integrity check failed")
            payload = target.read_bytes()
            cloud.put(bucket, f"backups/{stamp}/{path.name}", payload)
            manifest["databases"].append({"name": path.name, "sha256": hashlib.sha256(payload).hexdigest()})
        if not manifest["databases"]:
            manifest["note"] = "This deployed pilot has no durable runtime SQLite database yet."
        public = PUBLIC_CONFIG.read_bytes()
        cloud.put(bucket, f"backups/{stamp}/public.json", public)
        cloud.put(bucket, f"backups/{stamp}/manifest.json", json.dumps(manifest, sort_keys=True).encode())


def prepare_coordinator_environment(cloud, directory=Path("/run/factory/coordinator"), public_path=PUBLIC_CONFIG):
    """Fetch selected integrations as root into a per-release, coordinator-only file."""
    config = cloud.config
    revision = config.get("service_revision", "")
    workflow = config.get("coordinator_workflow", "pilot")
    mapping = config.get("coordinator_secret_env", {})
    secret_ids = config.get("secret_ids", {})
    secret_versions = config.get("secret_versions", {})
    allowed = COORDINATOR_SECRET_ENVS
    mapped_envs = set(mapping) if isinstance(mapping, dict) else set()
    optional_pair_refs = [mapped_envs & pair for pair in COORDINATOR_ADDITIONAL_SECRET_PAIRS]
    if (config.get("role") != "coordinator" or not re.fullmatch(r"[0-9a-f]{40}", revision)
            or workflow not in {"pilot", "linear"} or not isinstance(mapping, dict)
            or not set(mapping).issubset(allowed)
            or any(not isinstance(key, str) or key in {"worker_ssh", "model_auth", "git_read"}
                   or not isinstance(secret_ids, dict) or key not in secret_ids for key in mapping.values())
            or len(set(mapping.values())) != len(mapping)
            or (workflow == "linear" and not COORDINATOR_DEFAULT_SECRET_ENVS.issubset(mapped_envs))
            or (workflow == "linear" and any(pair and pair != expected for pair, expected in
                                               zip(optional_pair_refs, COORDINATOR_ADDITIONAL_SECRET_PAIRS)))
            or (workflow == "linear" and mapped_envs != (COORDINATOR_DEFAULT_SECRET_ENVS | set().union(*optional_pair_refs)))
            or (workflow == "pilot" and mapping)):
        raise ValueError("invalid coordinator integration configuration")

    mapped_keys = list(mapping.values())
    references = [secret_ids.get(key) for key in mapped_keys]
    if (any(not isinstance(reference, str) or not reference for reference in references)
            or len(set(references)) != len(references)):
        raise ValueError("coordinator secret references must be distinct")

    account = pwd.getpwnam("factory-coordinator")
    _assert_no_symlink_components(directory.parent)
    _check_directory(directory.parent, 0)
    directory.mkdir(mode=0o750, exist_ok=True)
    _check_directory(directory, 0)
    os.chown(directory, 0, account.pw_gid)
    os.chmod(directory, 0o750)
    environment = {}
    versions = {}
    fingerprints = {}
    for name, key in mapping.items():
        version = str(secret_versions.get(key, "")) if isinstance(secret_versions, dict) else ""
        if not re.fullmatch(r"[1-9][0-9]*", version):
            raise ValueError("coordinator credential version must be pinned")
        versions[name] = version
        fingerprints[name] = hashlib.sha256(f"{name}\n{key}\n{config['secret_ids'][key]}\n{version}".encode()).hexdigest()
    expected_pins = {"workflow": workflow, "secret_versions": versions, "secret_fingerprints": fingerprints,
                     "enable_linear_webhook": config.get("enable_linear_webhook", False)}
    # Keep the old runtime available for rollback. This alpha deployment binds
    # coordinator configuration changes to a distinct committed release.
    if public_path.exists():
        active = json.loads(_read_regular_file(public_path, expected_uid=0, expected_mode=0o644, limit=65536))
        active_pins = {"workflow": active.get("coordinator_workflow", "pilot"),
                       "secret_versions": active.get("coordinator_secret_versions", {}),
                       "secret_fingerprints": active.get("coordinator_secret_fingerprints", {}),
                       "enable_linear_webhook": active.get("enable_linear_webhook", False)}
        if active.get("service_revision") == revision and active_pins != expected_pins:
            raise ValueError("changed coordinator configuration requires a distinct service revision")
    destination = directory / (revision + ".json")
    if destination.exists() or destination.is_symlink():
        existing = json.loads(_read_regular_file(destination, expected_uid=0, expected_mode=0o640, limit=65536))
        if any(existing.get(key) != value for key, value in expected_pins.items()):
            raise ValueError("changed coordinator configuration requires a distinct service revision")
    for name, key in mapping.items():
        try:
            value = cloud.secret(key).decode("utf-8")
        except UnicodeDecodeError:
            raise ValueError("invalid coordinator credential encoding") from None
        if not value or len(value.encode()) > 16384 or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("invalid coordinator credential value")
        environment[name] = value
    payload = json.dumps({"service_revision": revision, "workflow": workflow, "environment": environment, "secret_versions": versions, "secret_fingerprints": fingerprints, "enable_linear_webhook": config.get("enable_linear_webhook", False)}).encode()
    _atomic_private_write(destination, payload, 0, account.pw_gid, mode=0o640)


def prepare_coordinator_environments(cloud, directory=Path("/run/factory/coordinator"),
                                     public_path=PUBLIC_CONFIG, snapshots=Path("/srv/factory/coordinator-config")):
    """Retain nonsecret reference pins so cold-boot rollback can reload old credentials."""
    _assert_no_symlink_components(snapshots.parent)
    _check_directory(snapshots.parent, 0)
    snapshots.mkdir(mode=0o700, exist_ok=True)
    _check_directory(snapshots, 0, private=True)
    if public_path.exists():
        active = json.loads(_read_regular_file(public_path, expected_uid=0, expected_mode=0o644, limit=65536))
        revision = active.get("service_revision", "")
        if active.get("coordinator_workflow") == "linear":
            if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
                raise ValueError("invalid rollback coordinator revision")
            previous = json.loads(_read_regular_file(snapshots / (revision + ".json"), expected_uid=0, expected_mode=0o600, limit=65536))
            if previous.get("service_revision") != revision:
                raise ValueError("rollback coordinator reference pins differ")
            prepare_coordinator_environment(Cloud(previous), directory, public_path)
    prepare_coordinator_environment(cloud, directory, public_path)
    # References and deployment settings only; never the fetched payloads.
    keys = ("project_id", "role", "service_revision", "coordinator_workflow", "coordinator_secret_env",
            "secret_ids", "secret_versions", "enable_linear_webhook")
    snapshot = {key: cloud.config[key] for key in keys if key in cloud.config}
    _atomic_private_write(snapshots / (cloud.config["service_revision"] + ".json"),
                          json.dumps(snapshot).encode(), 0, 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("pack")
    p.add_argument("repository")
    p.add_argument("output")
    p = sub.add_parser("get-release")
    p.add_argument("destination")
    p = sub.add_parser("secret")
    p.add_argument("key")
    p.add_argument("destination")
    sub.add_parser("model-auth")
    sub.add_parser("coordinator-env")
    sub.add_parser("ensure-hostkey")
    sub.add_parser("publish-hostkey")
    sub.add_parser("hostkeys")
    sub.add_parser("archive")
    sub.add_parser("backup")
    args = parser.parse_args()
    if args.command == "pack":
        return pack(args.repository, args.output)
    if os.geteuid() != 0:
        raise PermissionError("cloud transport is root-only")
    stat = CONFIG.stat()
    if stat.st_uid != 0 or stat.st_mode & 0o077:
        raise PermissionError("cloud config must be root-owned and mode 0600")
    cloud = Cloud(json.loads(CONFIG.read_text()))
    if args.command == "get-release":
        data = cloud.object(cloud.config["release_bucket"], cloud.config["release_object"])
        digest = hashlib.sha256(data).hexdigest()
        if digest != cloud.config["release_sha256"]:
            raise ValueError("release archive checksum mismatch")
        safe_unpack(data, args.destination)
        marker = json.loads((Path(args.destination) / "RELEASE.json").read_text())
        if marker["service_revision"] != cloud.config["service_revision"]:
            raise ValueError("release revision mismatch")
        (Path(args.destination) / ".verified-sha256").write_text(digest + "\n")
    elif args.command == "secret":
        path = Path(args.destination)
        with path.open("xb") as handle:
            os.chmod(path, 0o600)
            handle.write(cloud.secret(args.key))
    elif args.command == "model-auth":
        status = ensure_model_auth(cloud)
        print(f"worker model auth ready ({status})")
    elif args.command == "coordinator-env":
        prepare_coordinator_environments(cloud)
    elif args.command == "ensure-hostkey":
        if cloud.config.get("role") != "worker":
            raise ValueError("durable host key is available only on a worker")
        print(ensure_worker_host_key())
    elif args.command == "publish-hostkey":
        if cloud.config.get("role") != "worker":
            raise ValueError("host key publication is available only on a worker")
        public_key = ensure_worker_host_key()
        cloud.put(cloud.config["release_bucket"], "host-keys/" + cloud.config["instance_name"] + ".pub",
                  (public_key + "\n").encode())
    elif args.command == "hostkeys":
        records = []
        for worker in cloud.config["worker_hosts"]:
            raw = cloud.object(cloud.config["release_bucket"], "host-keys/" + worker["name"] + ".pub").decode().strip().split()
            if len(raw) < 2 or raw[0] != "ssh-ed25519":
                raise ValueError("worker host key must be ed25519")
            records.append(worker["ip"] + " " + " ".join(raw[:2]))
        Path("/run/factory/known_hosts").write_text("\n".join(records) + "\n")
    elif args.command == "archive":
        archive_runs(cloud)
    elif args.command == "backup":
        backup(cloud)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        command = sys.argv[1] if len(sys.argv) > 1 else "unknown"
        if command in ("archive", "backup") and os.geteuid() == 0:
            event(command + "_failed")
        # Error text is constrained; unexpected exceptions don't expose payloads.
        detail = f": {exc}" if isinstance(exc, (ValueError, RuntimeError, PermissionError)) else ""
        print(f"factory {command} failed ({type(exc).__name__}){detail}", file=sys.stderr)
        sys.exit(1)
