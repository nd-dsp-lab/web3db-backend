#!/usr/bin/env python3
"""
Benchmark Upload — Run on Server A (ND)
========================================

Uploads the dataset under every configuration in the sweep, then saves
all root CIDs and upload metadata to a JSON file.

SCP the output JSON to Server B, then run benchmark_query.py there.

Supported sweeps:
  - shard   : vary target_shard_size_kb  {32, 64, 128, 256, 512}
  - bf      : vary branching_factor      {2, 4, 8, 16, 32}
  - both    : run both sweeps

Usage (on Server A):
    python3 benchmark_upload.py --host http://localhost:8002 \\
        --sweep both --output upload_manifest.json

Then SCP the file to Server B:
    scp upload_manifest.json user@serverB:~/web3db-backend/app/scripts/

For NSDI 2027.
"""

import os
import sys
import time
import json
import argparse
import requests
from datetime import datetime

# ─── Defaults ────────────────────────────────────────────────────────────────

DEFAULT_HOST = "http://localhost:8002"
DATASET_FILE = "../dataset/employee_100k.csv"
DATASET_ROWS = 100_000

PARTITION_ATTRS = ["age", "salary_usd", "experience_years"]

SHARD_SIZES_KB = [32, 64, 128, 256, 512]
BRANCHING_FACTORS = [2, 4, 8, 16, 32]

# Fixed values when sweeping the other parameter
FIXED_BF = 4
FIXED_SHARD_KB = 64


# ─── Upload helper ───────────────────────────────────────────────────────────

def upload_dataset(host: str, filepath: str,
                   shard_size_kb: int, branching_factor: int) -> dict | None:
    """
    Upload dataset and return the full server response as a dict,
    or None on failure.
    """
    if not os.path.exists(filepath):
        print(f"    ✗ File not found: {filepath}")
        return None

    print(f"    Uploading shard_size={shard_size_kb}KB, bf={branching_factor} ...")
    start = time.time()
    try:
        with open(filepath, "rb") as f:
            resp = requests.post(
                f"{host}/upload-semantic/employee",
                files={"file": (os.path.basename(filepath), f, "text/csv")},
                params={
                    "partition_attributes": ",".join(PARTITION_ATTRS),
                    "target_shard_size_kb": shard_size_kb,
                    "branching_factor": branching_factor,
                },
                timeout=600,
            )
    except requests.ConnectionError:
        print(f"    ✗ Cannot connect to {host}")
        return None

    wall = time.time() - start

    if resp.status_code != 200:
        print(f"    ✗ HTTP {resp.status_code}: {resp.text[:300]}")
        return None

    r = resp.json()
    if r.get("status") != "success":
        print(f"    ✗ Upload failed: {r.get('error')}")
        return None

    print(f"    ✓ Uploaded in {wall:.1f}s — "
          f"{r['partition']['total_shards']} shards, "
          f"avg {r['partition']['avg_shard_size_kb']:.1f} KB, "
          f"DAG levels={r['dag']['levels']}, "
          f"root_cid={r['root_cid'][:16]}...")

    return {
        "root_cid":        r["root_cid"],
        "total_shards":    r["partition"]["total_shards"],
        "dag_levels":      r["dag"]["levels"],
        "total_data_bytes": r["dag"]["total_data_bytes"],
        "avg_shard_kb":    r["partition"]["avg_shard_size_kb"],
        "min_shard_kb":    r["partition"]["min_shard_size_kb"],
        "max_shard_kb":    r["partition"]["max_shard_size_kb"],
        "partition_ms":    r["timing"]["partition_ms"],
        "dag_build_ms":    r["timing"]["dag_build_ms"],
        "total_ms":        r["timing"]["total_ms"],
        "wall_seconds":    round(wall, 2),
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Upload all sweep configs on Server A, save manifest JSON")
    parser.add_argument("--host", default=DEFAULT_HOST,
                        help=f"Server A URL (default: {DEFAULT_HOST})")
    parser.add_argument("--dataset", default=DATASET_FILE,
                        help=f"CSV file (default: {DATASET_FILE})")
    parser.add_argument("--sweep", choices=["shard", "bf", "both"], default="both",
                        help="Which sweep to upload (default: both)")
    parser.add_argument("--output", default=None,
                        help="Output JSON path (default: upload_manifest_<ts>.json)")
    parser.add_argument("--shard-sizes", default=None,
                        help="Comma-separated shard sizes in KB (default: 32,64,128,256,512)")
    parser.add_argument("--branching-factors", default=None,
                        help="Comma-separated BFs (default: 2,4,8,16,32)")
    args = parser.parse_args()

    host = args.host
    dataset = args.dataset
    sweep_type = args.sweep

    shard_sizes = SHARD_SIZES_KB
    if args.shard_sizes:
        shard_sizes = [int(x.strip()) for x in args.shard_sizes.split(",")]

    branching_factors = BRANCHING_FACTORS
    if args.branching_factors:
        branching_factors = [int(x.strip()) for x in args.branching_factors.split(",")]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = args.output or f"upload_manifest_{timestamp}.json"

    # ── Connectivity check
    print(f"\n  Checking {host} ...")
    try:
        r = requests.get(f"{host}/health", timeout=10)
        if r.status_code == 200:
            print(f"  ✓ Server A healthy: {host}")
        else:
            print(f"  ⚠ HTTP {r.status_code}")
    except Exception as e:
        print(f"  ✗ Cannot reach {host}: {e}")
        sys.exit(1)

    # ── Banner
    print()
    print("╔" + "═" * 70 + "╗")
    print("║" + "  Benchmark Upload — Server A".center(70) + "║")
    print("║" + f"  Host: {host}".ljust(70) + "║")
    print("║" + f"  Dataset: {dataset}".ljust(70) + "║")
    print("║" + f"  Sweep: {sweep_type}".ljust(70) + "║")
    if sweep_type in ("shard", "both"):
        print("║" + f"  Shard sizes: {shard_sizes} KB  (BF fixed at {FIXED_BF})".ljust(70) + "║")
    if sweep_type in ("bf", "both"):
        print("║" + f"  BFs: {branching_factors}  (shard fixed at {FIXED_SHARD_KB} KB)".ljust(70) + "║")
    print("║" + f"  Output: {output_path}".ljust(70) + "║")
    print("║" + f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".ljust(70) + "║")
    print("╚" + "═" * 70 + "╝")

    manifest = {
        "created":          datetime.now().isoformat(),
        "upload_host":      host,
        "dataset":          os.path.basename(dataset),
        "dataset_rows":     DATASET_ROWS,
        "partition_attrs":  PARTITION_ATTRS,
        "sweep_type":       sweep_type,
        "shard_sweep":      [],   # list of configs
        "bf_sweep":         [],   # list of configs
    }

    # ── Shard size sweep uploads
    if sweep_type in ("shard", "both"):
        print(f"\n{'━' * 70}")
        print(f"  SHARD SIZE SWEEP (BF fixed at {FIXED_BF})")
        print(f"{'━' * 70}")
        for sk in shard_sizes:
            result = upload_dataset(host, dataset, sk, FIXED_BF)
            if result:
                manifest["shard_sweep"].append({
                    "shard_size_kb":    sk,
                    "branching_factor": FIXED_BF,
                    **result,
                })
            else:
                print(f"    ⚠ Skipping shard_size={sk}KB (upload failed)")

    # ── BF sweep uploads
    if sweep_type in ("bf", "both"):
        print(f"\n{'━' * 70}")
        print(f"  BRANCHING FACTOR SWEEP (shard fixed at {FIXED_SHARD_KB} KB)")
        print(f"{'━' * 70}")
        for bf in branching_factors:
            result = upload_dataset(host, dataset, FIXED_SHARD_KB, bf)
            if result:
                manifest["bf_sweep"].append({
                    "shard_size_kb":    FIXED_SHARD_KB,
                    "branching_factor": bf,
                    **result,
                })
            else:
                print(f"    ⚠ Skipping BF={bf} (upload failed)")

    total_configs = len(manifest["shard_sweep"]) + len(manifest["bf_sweep"])
    if total_configs == 0:
        print("\n  ✗ No successful uploads. Exiting.")
        sys.exit(1)

    # ── Save manifest
    with open(output_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\n{'═' * 70}")
    print(f"  ✓ Upload complete — {total_configs} configs saved to {output_path}")
    print(f"    Shard sweep: {len(manifest['shard_sweep'])} configs")
    print(f"    BF sweep:    {len(manifest['bf_sweep'])} configs")
    print(f"\n  Next steps:")
    print(f"    1. SCP the manifest to Server B:")
    print(f"       scp {output_path} user@serverB:~/web3db-backend/app/scripts/")
    print(f"    2. On Server B, run:")
    print(f"       python3 benchmark_query.py --manifest {output_path}")
    print(f"{'═' * 70}")


if __name__ == "__main__":
    main()
