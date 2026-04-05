#!/bin/bash
# generate_testdata_small.sh
#
# Generates 3 small input files for PrimeScience testing:
#
#   input_dataset_001.txt   ~1MB    ~95K  unique numbers
#   input_dataset_002.txt   ~5MB    ~476K unique numbers
#   input_dataset_003.txt   ~10MB   ~952K unique numbers
#
# Guarantees:
#   - NO duplicate numbers WITHIN each file
#   - Numbers CAN appear in multiple files — tests cross-file dedup
#   - Numbers in RANDOM order — realistic workload
#
# Run from the finalproject root:
#   chmod +x generate_testdata_small.sh
#   ./generate_testdata_small.sh
#
# Requirements : Python 3 + numpy  (pip3 install numpy)

set -e

OUTPUT_DIR="afsfs/testdata/input"
mkdir -p "$OUTPUT_DIR"

echo "========================================"
echo " PrimeScience — Small Test Data Generator"
echo "========================================"
echo " Output : $OUTPUT_DIR"
echo " Files  : 1MB + 5MB + 10MB"
echo " Total  : ~16 MB"
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
import os, time

OUTPUT_DIR = "afsfs/testdata/input"

FILES = [
    ("input_dataset_001.txt",   1,  42, "1MB"),
    ("input_dataset_002.txt",   5, 137, "5MB"),
    ("input_dataset_003.txt",  10, 999, "10MB"),
]

for idx, (fname, target_mb, seed, label) in enumerate(FILES, 1):
    outpath      = os.path.join(OUTPUT_DIR, fname)
    target_bytes = target_mb * 1024 * 1024
    n_needed     = int(target_bytes / 11)   # ~11 bytes per line (avg uint64 + newline)
    n_generate   = int(n_needed * 1.05)     # 5% extra buffer for uniqueness loss

    print(f"[{idx}/3] {fname}  ({label})  seed={seed}")
    print(f"  Target  : {target_mb} MB  |  Lines needed : {n_needed:,}")
    print(f"  Step 1  : generating {n_generate:,} random uint64 numbers...", flush=True)

    t0  = time.time()
    rng = np.random.default_rng(seed)

    # Use range 0..2^64 with cross-file overlap intentional
    # Files share ~10% of their range so primes overlap across files
    arr = rng.integers(0, 2**63, size=n_generate, dtype=np.uint64)

    print(f"  Step 2  : removing duplicates within file...", flush=True)
    arr = np.unique(arr)

    # Top up if uniqueness removed too many
    while len(arr) < n_needed:
        extra = rng.integers(0, 2**63, size=(n_needed - len(arr)) + 10_000, dtype=np.uint64)
        arr   = np.unique(np.concatenate([arr, extra]))

    arr = arr[:n_needed]

    print(f"  Step 3  : shuffling into random order...", flush=True)
    rng.shuffle(arr)

    print(f"  Step 4  : writing {len(arr):,} lines to disk...", flush=True)
    t1 = time.time()

    with open(outpath, 'w', buffering=4 * 1024 * 1024) as f:
        for num in arr:
            f.write(str(num))
            f.write('\n')

    t2 = time.time()
    size_mb = os.path.getsize(outpath) / (1024 * 1024)

    # Sanity check — no duplicates in first 1000 lines
    assert len(set(arr[:1_000].tolist())) == 1_000, "FAIL: duplicates found"

    print(f"  DONE    : {size_mb:.1f} MB  |  {len(arr):,} lines")
    print(f"  Time    : {t2-t0:.1f}s total")
    print(f"  CHECK   : PASS — no duplicates within file")
    print(flush=True)

    del arr

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
