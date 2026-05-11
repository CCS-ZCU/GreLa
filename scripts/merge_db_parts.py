#!/usr/bin/env python3

# run with: python merge_db_parts.py grela_v0.6.duckdb

import glob
import sys

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <base_filename>")
    sys.exit(1)

base = sys.argv[1]
parts = sorted(glob.glob(f"{base}.part*"))

with open(base, "wb") as out:
    for p in parts:
        with open(p, "rb") as f:
            out.write(f.read())
