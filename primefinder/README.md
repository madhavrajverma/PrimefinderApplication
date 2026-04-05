# Distributed Prime Finder — Part 2

A fault-tolerant distributed application for finding all unique prime numbers across large datasets stored on the AFS filesystem. Built from scratch in Go using gRPC. A coordinator assigns input files to parallel workers, collects results, deduplicates, and writes a final `primes.txt` to AFS. Chandy-Lamport snapshots enable recovery from any crash without restarting computation from scratch.


## What's Implemented

| Task | Feature |
|------|---------|
| **4** | Coordinator-worker architecture, file discovery, round-robin assignment, parallel dispatch, cross-file dedup, worker failover, output written to AFS |
| **5** | Chandy-Lamport consistent snapshots on AFS, coordinator resume from snapshot, per-worker snapshot on AFS, HTTP snapshot endpoint on each worker |


## Overview

The prime finder reads massive input files from the AFS distributed filesystem, tests every number for primality using deterministic Miller-Rabin, and writes a deduplicated sorted `primes.txt` back to AFS. Because primality testing is embarrassingly parallel — each number is independent — the computation scales linearly with the number of workers.

### Key Properties

| Property | Mechanism |
|----------|-----------|
| **Parallel processing** | One goroutine per worker; workers run files in parallel |
| **Worker failover** | Coordinator reassigns a failed worker's file to any surviving worker |
| **Cross-file dedup** | Coordinator maintains a global `seen` map; duplicates across files never reach output |
| **Correct primality** | Deterministic Miller-Rabin with 12 witnesses — correct for all 64-bit unsigned integers |
| **Snapshot recovery** | Coordinator saves state to AFS after every completed file; resumes from snapshot on restart |
| **AFS integration** | Workers are AFS clients — files fetched from AFS via streaming, cached locally, reads are pure local I/O |

**Flow:**
1. Coordinator connects to AFS, discovers all `input_dataset_NNN.txt` files
2. Coordinator checks AFS for an existing snapshot — resumes from it if found
3. Coordinator health-checks all workers, skips dead ones
4. Files assigned to workers round-robin; each worker dispatched in its own goroutine
5. Each worker: connects to AFS → streams file to local cache (1MB chunks) → tests every number → streams primes back in batches of 10,000
6. After each file completes: coordinator checkpoints its state to AFS (Chandy-Lamport)
7. Coordinator deduplicates all primes across all workers → writes `primes.txt` to AFS
8. AFS replicates `primes.txt` to all 3 servers before returning


## Primality Testing — `pkg/prime/miller_rabin.go`

Miller-Rabin with 12 fixed witnesses: `{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}`

This set is mathematically proven to be correct for all numbers below `3.3 × 10²⁴`, which covers the entire `uint64` range (`1.8 × 10¹⁹`). The test is deterministic — no false positives, no false negatives.

**Why not trial division?**
For a 64-bit number, trial division requires up to `√(2^64) ≈ 4 billion` iterations per number.
Miller-Rabin requires `O(log N)` multiplications — roughly 64 operations per number.
That is a ~60 million times speedup.

**Overflow-safe arithmetic:**
All modular arithmetic uses `mulmod` (Russian peasant multiplication) and `addmod` to prevent `uint64` overflow during `(a × b) mod m`.

```
IsPrime(n):
  1. Fast path: n < 2 → false; n ∈ {2,3,5,7} → true; divisible by 2,3,5 → false
  2. Write n-1 = 2^r × d  (factor out all 2s, d is odd)
  3. For each witness a in {2,3,5,7,11,13,17,19,23,29,31,37}:
       x = modpow(a, d, n)
       if x == 1 or x == n-1: continue   (probably prime for this witness)
       square x up to r-1 times:
         x = mulmod(x, x, n)
         if x == n-1: not composite for this witness
       if still composite after all squarings: return false (definitely composite)
  4. Passed all witnesses → return true (definitely prime for uint64 range)
```


## gRPC Protocol — `proto/prime.proto`

```protobuf
service WorkerService {
  // Server streaming — worker streams primes in batches of 10,000
  rpc ProcessFile (ProcessFileRequest) returns (stream ProcessFileResponse);
  rpc Health      (HealthRequest)      returns (HealthResponse);
}
```

### ProcessFile (server streaming)

| Field | Direction | Description |
|-------|-----------|-------------|
| `file_path` | coord → worker | e.g. `"input_dataset_001.txt"` |
| `afs_servers` | coord → worker | comma-separated AFS addresses |
| `cache_dir` | coord → worker | worker's local AFS cache dir |
| `worker_id` | coord → worker | e.g. `"w1"` |
| `primes` | worker → coord | batch of up to 10,000 primes per message |
| `count` | worker → coord | total numbers tested (set on final chunk) |
| `elapsed_ms` | worker → coord | processing time in milliseconds (set on final chunk) |
| `error` | worker → coord | non-empty string if processing failed |

Worker streams multiple `ProcessFileResponse` messages back to coordinator. Each message has up to 10,000 primes. No message size limit — works for any number of primes.

### Health

Simple ping-pong. Coordinator calls this before assigning any work to confirm the worker is alive. Dead workers are skipped rather than waited on.


## Coordinator Pipeline — `pkg/coordinator/coordinator.go`

```
coordinator.Run(cfg):

  1. afs.NewClient(cfg.AfsServers, cfg.CacheDir)
  2. discoverInputFiles() — Open each input_dataset_NNN.txt until missing
  3. snapshot.LoadCoordSnapshot() — resume from snapshot if found
  4. connectToWorkers() — Health RPC on each, skip dead ones
  5. assignFiles() — round-robin file assignment
  6. Dispatch: one goroutine per worker, resultCh collects results
  7. For each result: merge primes into global seen map, SaveCoordSnapshot
  8. writeFinalOutput() — dedupAndSort → CreateFile → Write → Close → replicated
  9. DeleteCoordSnapshot() — clean finish
```


## Chandy-Lamport Snapshots — `pkg/snapshot/snapshot.go`

### Why Chandy-Lamport simplifies for our topology

`ProcessFile` is a **synchronous streaming RPC** — the coordinator sends the request and collects all streamed responses before moving on. There are no messages in-flight between workers. This means:

- When the coordinator checkpoints, all channels are empty by definition
- No marker messages are needed
- The snapshot is simply the coordinator's local state at that moment

### What gets snapshotted

**Coordinator snapshot** (`snapshot_coordinator.json` on AFS):
```json
{
  "completed_files":   ["input_dataset_001.txt"],
  "pending_files":     ["input_dataset_002.txt", "input_dataset_003.txt"],
  "collected_primes":  [2, 3, 5, 7, 11, 13, ...],
  "timestamp_unix":    1712000000
}
```

**Worker snapshot** (`snapshot_w1.json` on AFS):
```json
{
  "worker_id":       "w1",
  "completed_files": ["input_dataset_001.txt"],
  "primes_found":    [2, 3, 5, 7, 11, ...],
  "timestamp_unix":  1712000000
}
```

Snapshots are written to AFS — automatically replicated to all 3 servers. Survive any crash.


## How to Run

---

### Option A — Docker

#### 1. Build everything

```bash
docker compose build
```

#### 2. Start AFS servers

```bash
docker compose up -d s1 s2 s3
docker compose ps
```

#### 3. Start workers

```bash
docker compose up -d worker1 worker2 worker3
docker compose ps
```

#### 4. Build coordinator and worker binaries inside tester

```bash
docker compose run --rm tester go build -o bin/coordinator ./cmd/coordinator
docker compose run --rm tester go build -o bin/worker      ./cmd/worker
```

#### 5. Normal run — coordinator processes all files

```bash
docker compose --profile run run --rm coordinator
# If image is stale, rebuild first:
# docker compose --profile run build --no-cache coordinator
```

#### 6. Verify output on all 3 servers

```bash
docker exec s1 wc -l /data/output/primes.txt
docker exec s2 wc -l /data/output/primes.txt
docker exec s3 wc -l /data/output/primes.txt

# Snapshot must be cleaned up after successful run
docker exec s1 ls /data/output/snapshot_*.json 2>/dev/null || echo "clean — no leftover snapshot"
```

#### Reset between tests

```bash
docker exec s1 sh -c 'rm -f /data/output/*.txt /data/output/snapshot_*.json'
docker exec s2 sh -c 'rm -f /data/output/*.txt /data/output/snapshot_*.json'
docker exec s3 sh -c 'rm -f /data/output/*.txt /data/output/snapshot_*.json'
```

---

#### Test 1A — Single Worker, Single File

**Proves:** One worker processes one file correctly. Output is valid primes, no duplicates, no misses.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester ./bin/coordinator \
  -afs s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001 \
  -cacheDir /tmp/coord-1a \
  -output primes.txt

docker compose run --rm tester go run tests/test1a/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test1a \
  -inputFile input_dataset_001.txt \
  -outputFile primes.txt
```

Expected: PASS output exists, PASS all prime, PASS no duplicates, PASS no misses.

---

#### Test 1B — Multiple Workers, Multiple Files

**Proves:** 3 workers process 3 files in parallel. Merged output has no cross-file duplicates.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester ./bin/coordinator \
  -afs s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -cacheDir /tmp/coord-1b \
  -output primes.txt

docker compose run --rm tester go run tests/test1b/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test1b \
  -outputFile primes.txt
```

Expected: PASS output exists, PASS all prime, PASS no cross-file duplicates, PASS no misses.

---

#### Test 2A — Server Crash During Coordinator Run

**Proves:** s1 dies mid-run, election elects new primary, coordinator completes, s1 syncs on restart.

**Terminal 1:**
```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester ./bin/coordinator \
  -afs s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -cacheDir /tmp/coord-2a \
  -output primes.txt
```

**Terminal 2** (after ~5 seconds):
```bash
sleep 5
docker compose stop s1
echo "s1 killed — election starting"
sleep 6
docker compose logs s2 | tail -5
docker compose logs s3 | tail -5
```

**After coordinator finishes:**
```bash
docker compose start s1
sleep 5

docker compose run --rm tester go run tests/test2a/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test2a \
  -outputFile primes.txt
```

Expected: PASS output exists, PASS all primes correct, PASS no duplicates, PASS s1 synced via SyncState.

---

#### Test 2B — Client Crash Before Close

**Proves:** Write() is local-only. No Close() = no StoreFile. Server never sees partial data.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test2b/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test2b
```

Expected: PASS v1 committed, PASS server has v1 (crash content never sent), PASS all 3 servers have v1.

---

#### Test 3A — Replication

**Proves:** Write via primary replicates to ALL 3 servers before returning success.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose --profile run run --rm coordinator

docker compose run --rm tester go run tests/test3/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test3a
```

Expected: PASS s1 has content, PASS s2 has content, PASS s3 has content, PASS all 3 servers identical.

---

#### Test 3B — Primary Failover

**Proves:** s1 dies mid-run, new primary elected, coordinator completes, s2/s3 consistent.

**Terminal 1:**
```bash
docker compose start s1
sleep 3
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester ./bin/coordinator \
  -afs s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -cacheDir /tmp/coord-3b \
  -output primes.txt
```

**Terminal 2** (after ~5 seconds):
```bash
sleep 5 && docker compose stop s1 && echo "s1 killed"
```

**After coordinator finishes:**
```bash
docker compose run --rm tester go run tests/test3b/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test3b \
  -outputFile primes.txt
```

Expected: PASS new primary elected, PASS write succeeded on new primary, PASS s2 and s3 have identical content.

---

#### Test 3C — Recovery and State Sync

**Proves:** s1 restarts and automatically syncs all files it missed via SyncState.

```bash
docker compose start s1
sleep 10

docker compose run --rm tester go run tests/test3c/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -cacheDir /tmp/test3c \
  -outputFile primes.txt

docker compose logs s1 | grep -i "synced\|SyncState"
```

Expected: PASS s1 has primes.txt via SyncState, PASS all 3 servers identical.

---

#### Test 4A — Coordinator Failure and Resume

**Proves:** Coordinator restores from Chandy-Lamport snapshot — skips completed files, reuses collected primes. Resumed output is identical to a full run.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test4a/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test4a
```

Expected: PASS full run N primes, PASS coordinator resumed from snapshot, PASS resumed output identical to full run, PASS no duplicates.

---

#### Test 4B — Single Worker Failure

**Proves:** Coordinator reassigns a dead worker's file to a surviving worker. Final output correct.

```bash
docker compose start s1 worker1 worker2 worker3
sleep 3
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test4b/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test4b \
  -docker=true

docker compose start worker1
```

Expected: PASS output matches ground truth, PASS no duplicates, PASS worker1 restarted.

---

#### Test 4C — Multiple Worker Failure

**Proves:** Majority workers killed. Coordinator snapshot + surviving worker3 produces correct output.

```bash
docker compose start s1 worker1 worker2 worker3
sleep 3
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test4c/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test4c \
  -docker=true

docker compose start worker1 worker2
```

Expected: PASS coordinator resumes from snapshot, PASS output matches ground truth, PASS no duplicates.

---

#### Test 5A — Throughput Scaling

**Proves:** More workers = faster processing. Prints a scaling table with 1, 2, 4, 8 workers.

```bash
docker compose up -d worker4 worker5 worker6 worker7 worker8

docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test5a/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -w1 worker1:6001 \
  -w2 worker1:6001,worker2:6001 \
  -w4 worker1:6001,worker2:6001,worker3:6001,worker4:6001 \
  -w8 worker1:6001,worker2:6001,worker3:6001,worker4:6001,worker5:6001,worker6:6001,worker7:6001,worker8:6001 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test5a
```

Expected: PASS all runs identical prime count, speedup improves with more workers, 1→8 workers speedup ~3x (bounded by 3 input files).

---

#### Test 5B — Large Dataset

**Proves:** System handles large 64-bit number datasets correctly with no duplicates, all primes valid, and no corruption.

```bash
docker exec s1 sh -c 'rm -f /data/output/*'
docker exec s2 sh -c 'rm -f /data/output/*'
docker exec s3 sh -c 'rm -f /data/output/*'

docker compose run --rm tester go run tests/test5b/main.go \
  -servers s1:50051,s2:50052,s3:50053 \
  -workers worker1:6001,worker2:6001,worker3:6001 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test5b \
  -count 500000
```

Expected: PASS uploaded to AFS, PASS all output entries are prime, PASS no duplicates, PASS AFS returned correct bytes with no corruption.

---

### Option B — Run Locally (No Docker)

#### 1. Build

```bash
cd afsfs
go build -o bin/server ./cmd/server

cd ../primefinder
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker      ./cmd/worker
```

#### 2. Generate test data

```bash
cd finalproject
chmod +x generate_testdata_small.sh
./generate_testdata_small.sh
# Creates input_dataset_001.txt (1MB), 002 (5MB), 003 (10MB)

# For large dataset tests (2A requires large files for mid-transfer kill):
pip3 install numpy
chmod +x generate_testdata.sh
./generate_testdata.sh   # ~5 min, creates 100MB + 500MB + 1GB
```

#### 3. Start AFS servers (3 terminals)

```bash
# Terminal 1
cd afsfs
./bin/server -id s1 -host localhost -port 50051 -primary=true \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1

# Terminal 2
./bin/server -id s2 -host localhost -port 50052 -primary=false \
  -peers s1=localhost:50051,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s2

# Terminal 3
./bin/server -id s3 -host localhost -port 50053 -primary=false \
  -peers s1=localhost:50051,s2=localhost:50052 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s3
```

#### 4. Start workers (3 terminals)

```bash
# Terminal 4
cd primefinder
./bin/worker -id w1 -port 6001 -snapshotPort 6100

# Terminal 5
./bin/worker -id w2 -port 6002 -snapshotPort 6101

# Terminal 6
./bin/worker -id w3 -port 6003 -snapshotPort 6102
```

#### 5. Normal run

```bash
cd primefinder
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

./bin/coordinator -afs $S -workers $W \
  -cacheDir /tmp/coord-cache -output primes.txt
```

#### 6. Verify output

```bash
wc -l afsfs/testdata/output-s1/primes.txt
diff afsfs/testdata/output-s1/primes.txt afsfs/testdata/output-s2/primes.txt && echo "s1==s2 ✓"
diff afsfs/testdata/output-s1/primes.txt afsfs/testdata/output-s3/primes.txt && echo "s1==s3 ✓"
```

#### Reset between tests

```bash
rm -f afsfs/testdata/output-s*/primes.txt
rm -f afsfs/testdata/output-s*/snapshot_*.json
rm -f afsfs/testdata/output-s*/test*.txt
```

---

#### Test 1A — Single Worker, Single File

```bash
cd primefinder
S="localhost:50051,localhost:50052,localhost:50053"

rm -f afsfs/testdata/output-s*/primes.txt

./bin/coordinator -afs $S -workers localhost:6001 \
  -cacheDir /tmp/coord-1a -output primes.txt

go run tests/test1a/main.go \
  -servers $S -cacheDir /tmp/test1a \
  -inputFile input_dataset_001.txt -outputFile primes.txt
```

Expected: PASS output exists, PASS all prime, PASS no duplicates, PASS no misses.

---

#### Test 1B — Multiple Workers, Multiple Files

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt

./bin/coordinator -afs $S -workers $W \
  -cacheDir /tmp/coord-1b -output primes.txt

go run tests/test1b/main.go \
  -servers $S -cacheDir /tmp/test1b -outputFile primes.txt
```

Expected: PASS output exists, PASS all prime, PASS no cross-file duplicates, PASS no misses.

---

#### Test 2A — Server Crash During Coordinator Run

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt
```

**Terminal 1:**
```bash
./bin/coordinator -afs $S -workers $W \
  -cacheDir /tmp/coord-2a -output primes.txt
```

**Terminal 2** (after ~5 seconds):
```bash
sleep 5
kill $(lsof -ti:50051 -sTCP:LISTEN)
echo "s1 killed"
```

**After coordinator finishes — restart s1 and verify:**
```bash
cd afsfs
./bin/server -id s1 -host localhost -port 50051 -primary=false \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1 &
sleep 8

cd ../primefinder
go run tests/test2a/main.go \
  -servers $S -cacheDir /tmp/test2a -outputFile primes.txt
```

Expected: PASS output exists, PASS all primes correct, PASS no duplicates, PASS s1 synced via SyncState.

---

#### Test 2B — Client Crash Before Close

```bash
S="localhost:50051,localhost:50052,localhost:50053"

go run tests/test2b/main.go -servers $S -cacheDir /tmp/test2b
```

Expected: PASS v1 committed, PASS server has v1 (crash content never sent), PASS all 3 servers have v1.

---

#### Test 3A — Replication

```bash
S="localhost:50051,localhost:50052,localhost:50053"

rm -f afsfs/testdata/output-s*/primes.txt

./bin/coordinator -afs $S \
  -workers localhost:6001,localhost:6002,localhost:6003 \
  -cacheDir /tmp/coord-3a -output primes.txt

go run tests/test3/main.go -servers $S -cacheDir /tmp/test3a
```

Expected: PASS s1 has content, PASS s2 has content, PASS s3 has content, PASS all 3 servers identical.

---

#### Test 3B — Primary Failover

**Terminal 1:**
```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt

./bin/coordinator -afs $S -workers $W \
  -cacheDir /tmp/coord-3b -output primes.txt
```

**Terminal 2** (after ~5 seconds):
```bash
sleep 5 && kill $(lsof -ti:50051 -sTCP:LISTEN) && echo "s1 killed"
```

**After coordinator finishes:**
```bash
go run tests/test3b/main.go \
  -servers $S -cacheDir /tmp/test3b -outputFile primes.txt
```

Expected: PASS new primary elected, PASS write succeeded, PASS s2 and s3 have identical content.

---

#### Test 3C — Recovery and State Sync

```bash
# Restart s1 as backup
cd afsfs
./bin/server -id s1 -host localhost -port 50051 -primary=false \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1 &
sleep 10

cd ../primefinder
go run tests/test3c/main.go \
  -servers $S -cacheDir /tmp/test3c -outputFile primes.txt
```

Expected: PASS s1 has primes.txt via SyncState, PASS all 3 servers identical.

---

#### Test 4A — Coordinator Failure and Resume

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt afsfs/testdata/output-s*/snapshot_*.json

go run tests/test4a/main.go \
  -servers $S -workers $W \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test4a
```

Expected: PASS full run N primes, PASS resumed output identical to full run, PASS no duplicates.

---

#### Test 4B — Single Worker Failure

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt afsfs/testdata/output-s*/snapshot_*.json

go run tests/test4b/main.go \
  -servers $S -workers $W \
  -coordBin ./bin/coordinator \
  -workerBin ./bin/worker \
  -cacheDir /tmp/test4b

# Restart worker1 after test
./bin/worker -id w1 -port 6001 -snapshotPort 6100 &
```

Expected: PASS output matches ground truth, PASS no duplicates, PASS worker1 restarted.

---

#### Test 4C — Multiple Worker Failure

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt afsfs/testdata/output-s*/snapshot_*.json

go run tests/test4c/main.go \
  -servers $S -workers $W \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test4c

# Restart workers after test
./bin/worker -id w1 -port 6001 -snapshotPort 6100 &
./bin/worker -id w2 -port 6002 -snapshotPort 6101 &
```

Expected: PASS coordinator resumes from snapshot, PASS output matches ground truth, PASS no duplicates.

---

#### Test 5A — Throughput Scaling

```bash
S="localhost:50051,localhost:50052,localhost:50053"

# Start extra workers for scaling test
./bin/worker -id w4 -port 6004 -snapshotPort 6104 &
./bin/worker -id w5 -port 6005 -snapshotPort 6105 &
./bin/worker -id w6 -port 6006 -snapshotPort 6106 &
./bin/worker -id w7 -port 6007 -snapshotPort 6107 &
./bin/worker -id w8 -port 6008 -snapshotPort 6108 &

rm -f afsfs/testdata/output-s*/primes.txt afsfs/testdata/output-s*/snapshot_*.json

go run tests/test5a/main.go \
  -servers $S \
  -w1 localhost:6001 \
  -w2 localhost:6001,localhost:6002 \
  -w4 localhost:6001,localhost:6002,localhost:6003,localhost:6004 \
  -w8 localhost:6001,localhost:6002,localhost:6003,localhost:6004,localhost:6005,localhost:6006,localhost:6007,localhost:6008 \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test5a
```

Expected: PASS all runs identical prime count, speedup improves with more workers.

---

#### Test 5B — Large Dataset

```bash
S="localhost:50051,localhost:50052,localhost:50053"
W="localhost:6001,localhost:6002,localhost:6003"

rm -f afsfs/testdata/output-s*/primes.txt afsfs/testdata/output-s*/snapshot_*.json

go run tests/test5b/main.go \
  -servers $S -workers $W \
  -coordBin ./bin/coordinator \
  -cacheDir /tmp/test5b \
  -count 500000
```

Expected: PASS uploaded to AFS, PASS all output entries are prime, PASS no duplicates, PASS AFS no corruption.

---

## Project Structure

```
primefinder/
├── cmd/
│   ├── coordinator/main.go     # coordinator binary — entry point, flag parsing
│   └── worker/main.go          # worker binary — gRPC server + HTTP snapshot endpoint
├── pkg/
│   ├── coordinator/
│   │   └── coordinator.go      # full pipeline: discover → assign → dispatch → dedup → write
│   ├── prime/
│   │   ├── miller_rabin.go     # IsPrime, mulmod, addmod, modpow
│   │   └── miller_rabin_test.go
│   └── snapshot/
│       └── snapshot.go         # WorkerSnapshot, CoordSnapshot, save/load on AFS
├── proto/prime.proto            # gRPC service definition (ProcessFile streaming, Health)
├── generated/prime/             # Generated gRPC Go stubs (do not edit)
├── tests/
│   ├── test1a/  test1b/         # Basic functionality tests
│   ├── test2a/  test2b/         # File server fault tolerance tests
│   ├── test3/   test3b/ test3c/ # Replication + failover + recovery tests
│   ├── test4a/  test4b/  test4c/ # Distributed prime finder fault tolerance tests
│   └── test5a/  test5b/         # Scaling and large dataset tests
├── Dockerfile
├── go.mod
└── go.sum
```


## Coordinator Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-afs` | Comma-separated AFS server addresses | `localhost:50051,localhost:50052,localhost:50053` |
| `-workers` | Comma-separated worker addresses | `localhost:6001,localhost:6002,localhost:6003` |
| `-cacheDir` | Coordinator's local AFS cache directory | `/tmp/prime-coord` |
| `-output` | Output filename on AFS | `primes.txt` |


## Worker Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-id` | Worker identifier (used in snapshots and logs) | `w1` |
| `-port` | gRPC listen port | `6001` |
| `-snapshotPort` | HTTP snapshot endpoint port | `6100` |


## Troubleshooting

**No workers available:**
```bash
curl http://localhost:6101/health   # worker1 HTTP health
curl http://localhost:6102/health   # worker2 HTTP health
curl http://localhost:6103/health   # worker3 HTTP health
```

**No input files found:**
```bash
ls afsfs/testdata/input/
# Files must be named input_dataset_001.txt, input_dataset_002.txt, etc.
```

**Coordinator says "RESUMING from snapshot" unexpectedly:**
```bash
# Delete leftover snapshot
docker exec s1 rm -f /data/output/snapshot_coordinator.json
docker exec s2 rm -f /data/output/snapshot_coordinator.json
docker exec s3 rm -f /data/output/snapshot_coordinator.json
# or locally:
rm -f afsfs/testdata/output-s*/snapshot_coordinator.json
```

**Worker OOM killed:**
```bash
docker inspect worker1 --format '{{.State.OOMKilled}}'
# If true — worker loaded full file into memory. Fixed in current version
# (uses io.Pipe + bufio.Scanner — constant memory regardless of file size)
```

**Ports already in use:**
```bash
kill $(lsof -ti:6001,6002,6003 -sTCP:LISTEN)
```