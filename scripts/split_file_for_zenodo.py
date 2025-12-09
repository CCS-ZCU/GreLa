#!/usr/bin/env python3
"""
Split a large file into fixed-size .partNNN files on disk.

Each part is named:

    <basename>.part000
    <basename>.part001
    ...

Users can reconstruct the original file after download with:

    cat <basename>.part* > <basename>
"""

import argparse
import math
import os
import sys
from typing import List


def split_file_to_parts(
    filepath: str,
    chunk_size_bytes: int,
) -> List[str]:
    """
    Split `filepath` into multiple files of size <= chunk_size_bytes.

    Returns a list of part filenames (basename.part000, basename.part001, ...).
    """
    base_name = os.path.basename(filepath)
    part_filenames: List[str] = []

    file_size = os.path.getsize(filepath)
    total_parts = math.ceil(file_size / chunk_size_bytes)

    print(
        f"Splitting '{filepath}' ({file_size:,} bytes) into {total_parts} parts "
        f"of up to {chunk_size_bytes} bytes each...",
        flush=True,
    )

    with open(filepath, "rb") as src:
        part_index = 0
        while True:
            part_name = f"{base_name}.part{part_index:03d}"
            bytes_written = 0

            with open(part_name, "wb") as dst:
                while bytes_written < chunk_size_bytes:
                    to_read = min(8 * 1024 * 1024, chunk_size_bytes - bytes_written)
                    chunk = src.read(to_read)
                    if not chunk:
                        break
                    dst.write(chunk)
                    bytes_written += len(chunk)

            if bytes_written == 0:
                # No more data, remove empty file and break
                try:
                    os.remove(part_name)
                except FileNotFoundError:
                    pass
                break

            part_filenames.append(part_name)
            print(
                f"Created part {part_index+1}/{total_parts}: {part_name} "
                f"({bytes_written:,} bytes)",
                flush=True,
            )

            part_index += 1

            if src.tell() >= file_size:
                break

    print(f"Finished splitting into {len(part_filenames)} part files.", flush=True)
    print(
        "\nTo reconstruct the original file after downloading all parts, run:\n"
        f"  cat {base_name}.part* > {base_name}\n"
    )
    return part_filenames


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Split a large file into fixed-size .partNNN files."
    )
    parser.add_argument(
        "--chunk-size-mb",
        type=int,
        default=100,
        help="Chunk size in megabytes (default: 100 MB).",
    )
    parser.add_argument(
        "file",
        help="Path to the large file (e.g. grela_v0.6.duckdb).",
    )

    args = parser.parse_args(argv)

    filepath = args.file
    chunk_size_mb = args.chunk_size_mb

    if not os.path.isfile(filepath):
        print(f"File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    if chunk_size_mb <= 0:
        print("chunk-size-mb must be > 0", file=sys.stderr)
        sys.exit(1)

    chunk_size = chunk_size_mb * 1024 * 1024

    split_file_to_parts(filepath, chunk_size)


if __name__ == "__main__":
    main()