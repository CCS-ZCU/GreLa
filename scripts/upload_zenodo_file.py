#!/usr/bin/env python3
"""
Upload one or more files to an existing Zenodo deposit using the bucket API.

Extremely verbose version for debugging:
- Prints when script loads
- Prints when main() starts
- Prints before and after calling upload_file_to_bucket
- Shows a tqdm progress bar and kB/s throughput
"""

import argparse
import os
import re
import sys
import time
from typing import Optional, Tuple

import requests
from requests.exceptions import SSLError, ConnectionError
from tqdm import tqdm

print("[DEBUG] upload_zenodo_file.py loaded", flush=True)


# ----------------- Helper functions ----------------- #

def parse_deposit_id_from_url(deposit_url: str) -> int:
    match = re.search(r"(\d+)(?:/)?$", deposit_url.strip())
    if not match:
        raise ValueError(f"Could not parse deposit ID from URL: {deposit_url}")
    return int(match.group(1))


def infer_api_base_from_url(deposit_url: str) -> str:
    if "sandbox.zenodo.org" in deposit_url:
        return "https://sandbox.zenodo.org/api"
    return "https://zenodo.org/api"


def get_bucket_url(api_base: str, token: str, deposit_id: int) -> str:
    print("[DEBUG] get_bucket_url() called", flush=True)
    url = f"{api_base}/deposit/depositions/{deposit_id}"
    headers = {"Authorization": f"Bearer {token}"}

    print(f"[DEBUG] GET {url}", flush=True)
    resp = requests.get(url, headers=headers, timeout=30)

    print(f"[DEBUG] deposit metadata status: {resp.status_code}", flush=True)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch deposit metadata (status {resp.status_code}): "
            f"{resp.text}"
        )

    data = resp.json()
    links = data.get("links", {})
    bucket_url = links.get("bucket")
    print(f"[DEBUG] links: {links}", flush=True)
    if not bucket_url:
        raise RuntimeError(
            "No 'bucket' link found in deposition metadata. "
            "Is the record still editable?"
        )
    print(f"[DEBUG] bucket_url: {bucket_url}", flush=True)
    return bucket_url


# ----------------- The upload function ----------------- #

def upload_file_to_bucket(
    bucket_url: str,
    token: str,
    filepath: str,
    target_name: Optional[str] = None,
    max_retries: int = 3,
    retry_wait: int = 60,
) -> Tuple[bool, str]:
    """
    Upload a single file to the Zenodo bucket using the new files API,
    with a progress bar and basic retry logic for TLS/network errors.
    """
    print("[DEBUG] upload_file_to_bucket() entered", flush=True)

    if target_name is None:
        target_name = os.path.basename(filepath)

    if not os.path.isfile(filepath):
        msg = f"File not found: {filepath}"
        print(f"[DEBUG] {msg}", flush=True)
        return False, msg

    url = f"{bucket_url.rstrip('/')}/{target_name}"
    headers = {"Authorization": f"Bearer {token}"}
    file_size = os.path.getsize(filepath)

    print(
        f"[DEBUG] Preparing upload:\n"
        f"  bucket_url: {bucket_url}\n"
        f"  target_url: {url}\n"
        f"  file:       {filepath} ({file_size:,} bytes)\n",
        flush=True,
    )

    chunk_size = 8 * 1024 * 1024  # 8 MB

    for attempt in range(1, max_retries + 1):
        print(
            f"[DEBUG] Starting attempt {attempt}/{max_retries} for '{filepath}'",
            flush=True,
        )

        try:
            start_time = time.time()
            bytes_sent = 0

            with open(filepath, "rb") as fp, tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=target_name,
                mininterval=0.5,
                ascii=True,
            ) as t:

                def gen():
                    nonlocal bytes_sent, start_time
                    while True:
                        chunk = fp.read(chunk_size)
                        if not chunk:
                            break
                        bytes_sent += len(chunk)
                        t.update(len(chunk))

                        elapsed = max(time.time() - start_time, 1e-6)
                        speed_kb = bytes_sent / 1024 / elapsed
                        t.set_postfix_str(f"{speed_kb:6.1f} kB/s")

                        yield chunk

                print(f"[DEBUG] Sending PUT to {url}", flush=True)
                resp = requests.put(
                    url,
                    data=gen(),
                    headers=headers,
                    timeout=(30, 600),
                )

            print(f"[DEBUG] Upload HTTP status: {resp.status_code}", flush=True)

            if resp.status_code in (200, 201):
                msg = f"Uploaded '{filepath}' as '{target_name}'"
                print(f"[DEBUG] {msg}", flush=True)
                return True, msg

            # Non-network error
            try:
                details = resp.json()
            except Exception:
                details = resp.text
            msg = (
                f"Upload failed for '{filepath}' (status {resp.status_code}). "
                f"Response: {details}"
            )
            print(f"[DEBUG] {msg}", flush=True)
            return False, msg

        except (SSLError, ConnectionError) as e:
            print(f"[DEBUG] Network/SSL error during upload: {e}", flush=True)
            if attempt < max_retries:
                print(
                    f"[DEBUG] Waiting {retry_wait} seconds before retrying...",
                    flush=True,
                )
                time.sleep(retry_wait)
            else:
                msg = (
                    f"Giving up after {max_retries} attempts due to "
                    f"repeated network/SSL errors."
                )
                print(f"[DEBUG] {msg}", flush=True)
                return False, msg

        except Exception as e:
            msg = f"Unexpected error during upload: {e!r}"
            print(f"[DEBUG] {msg}", flush=True)
            return False, msg

    return False, "Unexpected error in upload loop (no attempts succeeded)"


# ----------------- main() ----------------- #

def main(argv=None):
    print("[DEBUG] main() starting", flush=True)

    parser = argparse.ArgumentParser(
        description="Upload file(s) to an existing Zenodo deposit using the bucket API."
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Zenodo personal access token (with deposit:write and deposit:actions).",
    )
    parser.add_argument(
        "--deposit-url",
        required=True,
        help=(
            "Zenodo deposit URL, e.g. "
            "'https://zenodo.org/uploads/17866143' "
            "or 'https://zenodo.org/deposit/17866143'."
        ),
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="One or more file paths to upload.",
    )

    args = parser.parse_args(argv)

    token = args.token
    deposit_url = args.deposit_url
    filepaths = args.files

    print(f"[DEBUG] Parsed args:", flush=True)
    print(f"  token:       {'***redacted***'}", flush=True)
    print(f"  deposit_url: {deposit_url}", flush=True)
    print(f"  files:       {filepaths}", flush=True)

    try:
        deposit_id = parse_deposit_id_from_url(deposit_url)
    except ValueError as e:
        print(f"[DEBUG] Error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    api_base = infer_api_base_from_url(deposit_url)

    print(f"[DEBUG] Using API base: {api_base}", flush=True)
    print(f"[DEBUG] Deposit ID: {deposit_id}", flush=True)

    try:
        bucket_url = get_bucket_url(api_base, token, deposit_id)
    except Exception as e:
        print(f"[DEBUG] Error while fetching bucket URL: {e}", file=sys.stderr, flush=True)
        sys.exit(1)

    print(f"[DEBUG] Bucket URL: {bucket_url}", flush=True)

    any_fail = False
    for path in filepaths:
        print(f"[DEBUG] Calling upload_file_to_bucket() for '{path}'", flush=True)
        ok, msg = upload_file_to_bucket(bucket_url, token, path)
        print(f"[DEBUG] upload_file_to_bucket returned ok={ok}, msg={msg}", flush=True)
        if not ok:
            any_fail = True

    if any_fail:
        print("[DEBUG] One or more uploads failed", flush=True)
        sys.exit(1)
    else:
        print("[DEBUG] All uploads completed successfully.", flush=True)


if __name__ == "__main__":
    print("[DEBUG] __main__ guard triggered, calling main()", flush=True)
    main()