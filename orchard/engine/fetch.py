# orchard/engine/fetch.py

import hashlib
import io
import os
import tarfile
from pathlib import Path

import requests
from filelock import FileLock

MANIFEST_URL = "https://prod.proxy.ing/functions/v1/get-release-manifest"
DEFAULT_CHANNEL = "stable"
ORCHARD_HOME = Path.home() / ".orchard"


def get_engine_path() -> Path:
    """Return path to the engine binary, downloading if necessary."""
    # Check for local dev override first (always wins)
    local_build = os.environ.get("PIE_LOCAL_BUILD")
    if local_build:
        local_path = Path(local_build) / "bin" / "proxy_inference_engine"
        if local_path.exists():
            return local_path
        raise FileNotFoundError(
            f"PIE_LOCAL_BUILD set but binary not found: {local_path}"
        )

    binary_path = ORCHARD_HOME / "bin" / "proxy_inference_engine"

    # Fast path: already installed
    if binary_path.exists():
        return binary_path

    # Need to download - use lock to prevent concurrent downloads corrupting the binary
    ORCHARD_HOME.mkdir(parents=True, exist_ok=True)
    lock_path = ORCHARD_HOME / "install.lock"

    with FileLock(str(lock_path), timeout=300):  # 5 min timeout for slow connections
        # Double-check: another process may have installed while we waited
        if binary_path.exists():
            return binary_path

        print("Orchard engine not found. Downloading...")
        download_engine()

    if not binary_path.exists():
        raise RuntimeError("Download completed but binary not found")

    return binary_path


def download_engine(channel: str = DEFAULT_CHANNEL, version: str | None = None):
    """Download and install the engine from Supabase."""

    # 1. Fetch manifest
    resp = requests.get(MANIFEST_URL, params={"channel": channel})
    resp.raise_for_status()
    manifest = resp.json()

    # 2. Resolve version
    if version is None:
        version = manifest["latest"]

    if version not in manifest["versions"]:
        raise ValueError(f"Version {version} not found in {channel} channel")

    info = manifest["versions"][version]
    url = info["url"]
    expected_sha256 = info["sha256"]

    print(f"Downloading Orchard engine {version}...")

    # 3. Download
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    content = resp.content

    # 4. Verify SHA256
    actual_sha256 = hashlib.sha256(content).hexdigest()
    if expected_sha256 and actual_sha256 != expected_sha256:
        raise RuntimeError(
            f"SHA256 mismatch!\n"
            f"  Expected: {expected_sha256}\n"
            f"  Got:      {actual_sha256}"
        )

    # 5. Extract to ~/.orchard/
    ORCHARD_HOME.mkdir(parents=True, exist_ok=True)

    with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as tar:
        tar.extractall(ORCHARD_HOME)

    # 6. Make binary executable
    binary_path = ORCHARD_HOME / "bin" / "proxy_inference_engine"
    binary_path.chmod(0o755)

    # 7. Write version file
    version_file = ORCHARD_HOME / "version.txt"
    version_file.write_text(version or "")

    print(f"Orchard engine {version} installed to {ORCHARD_HOME}")


def get_installed_version() -> str | None:
    """Return currently installed version, or None if not installed."""
    version_file = ORCHARD_HOME / "version.txt"
    if version_file.exists():
        return version_file.read_text().strip()
    return None


def check_for_updates(channel: str = DEFAULT_CHANNEL) -> str | None:
    """Return latest version if newer than installed, else None."""
    installed = get_installed_version()
    if not installed:
        return None

    resp = requests.get(MANIFEST_URL, params={"channel": channel})
    resp.raise_for_status()
    manifest = resp.json()
    latest = manifest["latest"]

    if latest != installed:
        return latest
    return None
