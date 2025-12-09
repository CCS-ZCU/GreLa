#!/usr/bin/env python3
"""
Upload already-split .partNNN files to an existing Zenodo deposition
using the bucket API.

Assumptions:
- You have files like: <basename>.part000, <basename>.part001, ...
- All part files are in the current working directory.
- You want to upload them all to the same deposition.

This script does NOT create the chunks, it only uploads what exists.
"""

import argparse
import glob
import os
import re
import sys
from typing import List, Tuple

import requests
from requests.exceptions import SSLError, ConnectionError


# ---------- Zenodo API helpers ----------

def parse_deposit_id_from_url(deposit_url: str) -> int:
    """Extract numeric deposit ID from URLs like .../uploads/17866143 or .../deposit/17866143."""
    match = re.search(r"(\d+)(?:/)?$", deposit_url.strip())
    if not match:
        raise ValueError(f"Could not parse deposit ID from URL: {deposit_url}")
    return int(match.group(1))


def infer_api_base_from_url(deposit_url: str) -> str:
    """Infer the correct /api base from a web URL."""
    if "sandbox.zenodo.org" in deposit_url:
        return "https://sandbox.zenodo.org/api"
    return "https://zenodo.org/api"


def get_bucket_url(api_base: str, token: str, deposit_id: int) -> str:
    """Fetch deposition metadata and extract the bucket URL."""
    url = f"{api_base}/deposit/depositions/{deposit_id}"
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.get(url, headers=headers, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch deposit metadata (status {resp.status_code}): {resp.text}"
        )

    data = resp.json()
    links = data.get("links", {})
    bucket_url = links.get("bucket")
    if not bucket_url:
        raise RuntimeError(
            "No 'bucket' link found in deposition metadata. "
            "Is the record still editable?"
        )
    return bucket_url


# ---------- Upload a single part file ----------

def upload_part_file(
    bucket_url: str,
    token: str,
    part_path: str,
    max_retries: int = 3,
    retry_wait: int = 60,
) -> Tuple[bool, str]:
    """
    Upload a single on-disk part file to Zenodo using the documented pattern:

        with open(path, "rb") as fp:
            r = requests.put("%s/%s" % (bucket_url, filename), data=fp, headers=headers)
    """
    import time as _time

    filename = os.path.basename(part_path)
    url = f"{bucket_url.rstrip('/')}/{filename}"
    file_size = os.path.getsize(part_path)

    headers = {"Authorization": f"Bearer {token}"}

    for attempt in range(1, max_retries + 1):
        print(
            f"Uploading '{filename}' ({file_size:,} bytes) to {url} "
            f"(attempt {attempt}/{max_retries})...",
            flush=True,
        )

        try:
            with open(part_path, "rb") as fp:
                # This follows Zenodo's official example: file object as data
                resp = requests.put(
                    url,
                    data=fp,
                    headers=headers,
                    timeout=(30, 600),
                )

            print(f"HTTP status for '{filename}': {resp.status_code}", flush=True)

            if resp.status_code in (200, 201):
                return True, f"OK: {filename}"

            try:
                details = resp.json()
            except Exception:
                details = resp.text
            msg = (
                f"Upload failed for '{filename}' (status {resp.status_code}). "
                f"Response: {details}"
            )
            print(msg, flush=True)
            return False, msg

        except (SSLError, ConnectionError) as e:
            print(f"Network/SSL error while uploading '{filename}': {e}", flush=True)
            if attempt < max_retries:
                print(f"Waiting {retry_wait} seconds before retrying...", flush=True)
                _time.sleep(retry_wait)
            else:
                msg = (
                    f"Giving up on '{filename}' after {max_retries} attempts "
                    f"due to repeated network/SSL errors."
                )
                print(msg, flush=True)
                return False, msg

        except Exception as e:
            msg = f"Unexpected error while uploading '{filename}': {e!r}"
            print(msg, flush=True)
            return False, msg

    return False, f"Unexpected error in upload loop for '{filename}'"


# ---------- CLI main ----------

def main(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Upload existing .partNNN files to an existing Zenodo deposition "
            "using the bucket API."
        )
    )
    parser.add_argument(
        "--token",
        required=True,
        help="Zenodo access token (with deposit:write and deposit:actions).",
    )
    parser.add_argument(
        "--deposit-url",
        required=True,
        help="Zenodo deposit URL, e.g. 'https://zenodo.org/uploads/17866143'.",
    )
    parser.add_argument(
        "--base-name",
        required=True,
        help=(
            "Base name of the original file, e.g. 'grela_v0.6.duckdb'. "
            "The script will upload all files matching '<base-name>.part*' "
            "in the current directory."
        ),
    )

    args = parser.parse_args(argv)

    token = args.token
    deposit_url = args.deposit_url
    base_name = args.base_name

    api_base = infer_api_base_from_url(deposit_url)

    try:
        deposit_id = parse_deposit_id_from_url(deposit_url)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"API base:   {api_base}")
    print(f"Deposit ID: {deposit_id}")
    print(f"Base name:  {base_name}")
    print()

    try:
        bucket_url = get_bucket_url(api_base, token, deposit_id)
    except Exception as e:
        print(f"Error while fetching bucket URL: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Bucket URL: {bucket_url}")
    print()

    # Find existing part files in the current directory
    pattern = f"{base_name}.part*"
    part_paths: List[str] = sorted(glob.glob(pattern))

    if not part_paths:
        print(f"No part files found matching pattern: {pattern}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(part_paths)} part files:")
    for p in part_paths:
        print(f"  {p} ({os.path.getsize(p):,} bytes)")
    print()

    if len(part_paths) > 100:
        print(
            f"Warning: {len(part_paths)} part files found, "
            f"but Zenodo allows max 100 files per record.",
            file=sys.stderr,
        )

    any_fail = False
    for idx, part_path in enumerate(part_paths, start=1):
        print(f"\n=== Uploading part {idx}/{len(part_paths)}: {part_path} ===", flush=True)
        ok, msg = upload_part_file(bucket_url, token, part_path)
        print(msg, flush=True)
        if not ok:
            any_fail = True
            break

    if any_fail:
        print("\nSome parts failed to upload.", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll parts uploaded successfully.")
        print(
            f"\nAfter users download all parts, they can reconstruct the file with:\n"
            f"  cat {base_name}.part* > {base_name}\n"
        )


if __name__ == "__main__":
    main()