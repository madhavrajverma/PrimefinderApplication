#!/bin/bash
# generate_testdata.sh
#
# Generates 3 input files for PrimeScience testing:
#
#   input_dataset_001.txt   ~100MB   ~9.5M  unique numbers per file
#   input_dataset_002.txt   ~500MB   ~47.7M unique numbers per file
#   input_dataset_003.txt   ~1GB     ~97.6M unique numbers per file
#
# Guarantees:
#   - NO duplicate numbers WITHIN each file
#   - Numbers CAN appear in multiple files — intentional, tests cross-file dedup
#   - Numbers in RANDOM order — realistic workload
#
# Run ONCE from the finalproject root before any tests:
#   chmod +x generate_testdata.sh
#   ./generate_testdata.sh
#
# Requirements : Python 3 + numpy  (pip3 install numpy)
# Expected time : ~5 minutes
# Disk space    : ~1.6 GB

set -e

OUTPUT_DIR="afsfs/testdata/input"
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo " PrimeScience — Test Data Generator"
echo "========================================"
echo " Output : $OUTPUT_DIR"
echo " Files  : 100MB + 500MB + 1GB"
echo " Total  : ~1.6 GB"
echo " Time   : ~5 minutes"
echo ""
echo " Within each file  : NO duplicate numbers"
echo " Across files      : same number CAN appear in multiple files"
echo " Why               : coordinator deduplicates primes across files"
echo "========================================"
echo ""

python3 -c "import numpy" 2>/dev/null || {
    echo "ERROR: numpy not found. Install with: pip3 install numpy"
    exit 1
}
echo "numpy : OK"
echo ""

python3 << 'PYEOF'
import numpy as np
import os, sys, time

OUTPUT_DIR = "afsfs/testdata/input"

FILES = [
    ("input_dataset_001.txt",  100,  42,  "100MB"),
    ("input_dataset_002.txt",  500, 137,  "500MB"),
    ("input_dataset_003.txt", 1024, 999,   "1GB"),
]

for idx, (fname, target_mb, seed, label) in enumerate(FILES, 1):
    outpath      = os.path.join(OUTPUT_DIR, fname)
    target_bytes = target_mb * 1024 * 1024
    n_needed     = int(target_bytes / 11)   # ~11 bytes per line
    n_generate   = int(n_needed * 1.02)     # 2% extra buffer

    print(f"[{idx}/3] {fname}  ({label})  seed={seed}")
    print(f"  Target  : {target_mb} MB  |  Lines needed : {n_needed:,}")
    print(f"  Step 1  : generating {n_generate:,} random uint64 numbers...", flush=True)

    t0  = time.time()
    rng = np.random.default_rng(seed)
    arr = rng.integers(0, 2**64, size=n_generate, dtype=np.uint64)

    print(f"  Step 2  : removing duplicates within file...", flush=True)
    arr = np.unique(arr)

    # Top up if uniqueness removed too many
    while len(arr) < n_needed:
        extra = rng.integers(0, 2**64, size=(n_needed - len(arr)) + 10_000, dtype=np.uint64)
        arr   = np.unique(np.concatenate([arr, extra]))

    arr = arr[:n_needed]   # trim to exact count

    print(f"  Step 3  : shuffling into random order...", flush=True)
    rng.shuffle(arr)

    t1 = time.time()
    print(f"  Step 4  : writing {len(arr):,} lines to disk...", flush=True)

    t2 = time.time()
    with open(outpath, 'w', buffering=16 * 1024 * 1024) as f:
        for num in arr:
            f.write(str(num))
            f.write('\n')
    t3 = time.time()

    size_mb = os.path.getsize(outpath) / (1024 * 1024)

    # Quick sanity check — verify no duplicates in first 10000 lines
    assert len(set(arr[:10_000].tolist())) == 10_000, "FAIL: duplicates found in file"

    print(f"  DONE    : {size_mb:.0f} MB  |  {len(arr):,} lines")
    print(f"  Time    : {t1-t0:.1f}s generate  +  {t3-t2:.1f}s write  =  {t3-t0:.1f}s total")
    print(f"  CHECK   : PASS — no duplicates within file")
    print(flush=True)

    del arr   # free RAM before next file

print("All 3 files generated successfully.")
PYEOF

echo "========================================"
echo " All done"
echo "========================================"
echo ""
ls -lh "$OUTPUT_DIR"/input_dataset_*.txt
echo ""
echo "Total:"
du -sh "$OUTPUT_DIR"
echo ""
echo "What the coordinator does with these files:"
echo "  - Assigns one file per worker (round-robin)"
echo "  - Each worker finds primes in its file independently"
echo "  - Coordinator merges all primes into one global seen map"
echo "  - Writes primes.txt with each prime exactly once"
echo "  - Same prime in file 001 and file 002 appears once in primes.txt"
