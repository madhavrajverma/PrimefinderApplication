# Distributed prime number finding application

A distributed platform for discovering large prime numbers across Gigabytes of numerical data. The platform combines a fault-tolerant distributed filesystem with an embarrassingly parallel prime-finding application, both implemented from scratch in Go using gRPC.

**No external consensus libraries. No MapReduce frameworks. No shared databases. Built from first principles.**

---

## What this project is

PrimeScience is a two-part distributed systems project:

**[Part 1 — AFS Distributed Filesystem](./afsfs/README.md)** is a user-space filesystem inspired by the Andrew File System. It stores input datasets and output results across three replica servers. Clients get a simple `Open / Read / Write / Close` API that looks like local I/O but is backed by a fault-tolerant replicated storage layer.

**[Part 2 — Distributed Prime Finder](./primefinder/README.md)** is the application layer. A coordinator assigns large input files to parallel workers. Each worker fetches its file from AFS, tests every number for primality using deterministic Miller-Rabin, and returns results. The coordinator deduplicates, sorts, and writes the final `primes.txt` to AFS. Chandy-Lamport snapshots enable crash recovery without restarting computation from zero.


Workers are stateless compute nodes that are also AFS clients  they fetch input files directly from AFS, process them locally, and return results. The coordinator owns global deduplication and checkpointing. AFS handles all persistence and replication transparently.

---

## Five success metrics

| Metric | How it is satisfied |
|--------|---------------------|
| **Scale** | Files distributed across N workers. Each worker processes its file locally after one AFS fetch. Adding workers scales linearly — primality testing is embarrassingly parallel. |
| **Survive** | 3 AFS servers survive any one failure. Bully election promotes a backup to primary in ~3 seconds. Clients auto-reconnect. Workers are reassigned on crash. |
| **Recover** | Coordinator saves a Chandy-Lamport snapshot to AFS after every completed file. On restart it resumes from the snapshot — skips completed files, reuses collected primes. Maximum work lost = one file. |
| **Perform** | 3 workers process 3 files simultaneously. Throughput scales linearly with number of workers. Test 5A demonstrates measured speedup with a scaling table. |
| **Deliver** | Deterministic Miller-Rabin with 12 witnesses — correct for all 64-bit unsigned integers. Two-level deduplication (worker + coordinator). AFS whole-file semantics prevent partial writes. Output replicated to all 3 servers. |

---

## Part 1 — AFS Distributed Filesystem

> Full documentation: **[afsfs/README.md](./afsfs/README.md)**

### What it does

Stores large input datasets and output result files across a cluster of servers. Gives clients a simple file API that looks like local I/O but is backed by fault-tolerant replicated storage.

### Key features

| Feature | Mechanism |
|---------|-----------|
| Remote file access | Open, Read, Write, Close, Create, Delete over gRPC |
| Whole-file caching | Client fetches entire file on first open — all subsequent reads are local disk I/O |
| Cache consistency | `TestAuth` RPC checks server version before trusting local copy |
| High availability | 3 replica servers — system survives any one server failure |
| Automatic failover | Heartbeat-based failure detection + lowest-ID bully election |
| Self-healing | Recovered server pulls all missed files from current primary via `SyncState` |
| Idempotency | Every mutating RPC carries `(clientID, reqSeq)` — server-side dedup table prevents double execution on retry |
| Atomic writes | `WriteFile + Rename` on server — partial writes never corrupt the stored file |

### Tasks implemented

| Task | Feature |
|------|---------|
| **1A** | Open, Read, Write, Close, Create, Delete over gRPC — `proto/afs.proto` |
| **1B** | Whole-file caching with `TestAuth` version validation |
| **2** | At-least-once RPCs, client crash safety, idempotency via `clientID + reqSeq` |
| **3** | Primary-backup replication, heartbeat failure detection, bully election, client failover, server recovery sync |

### Quick start

```bash
cd afsfs
go build -o bin/server ./cmd/server

# Start 3 servers
./bin/server -id s1 -host localhost -port 50051 -primary=true \
  -peers s2=localhost:50052,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s1

./bin/server -id s2 -host localhost -port 50052 -primary=false \
  -peers s1=localhost:50051,s3=localhost:50053 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s2

./bin/server -id s3 -host localhost -port 50053 -primary=false \
  -peers s1=localhost:50051,s2=localhost:50052 \
  -inputDir ./testdata/input -outputDir ./testdata/output-s3
```

---

## Part 2 — Distributed Prime Finder

> Full documentation: **[primefinder/README.md](./primefinder/README.md)**

> Test suite documentation: **[primefinder/tests/README.md](./primefinder/README.md)**

### What it does

Reads large input files from AFS, tests every number for primality in parallel across multiple workers, deduplicates all results, and writes a sorted `primes.txt` back to AFS. Crash recovery is handled via Chandy-Lamport snapshots stored on AFS.

### Key features

| Feature | Mechanism |
|---------|-----------|
| Parallel processing | One goroutine per worker — workers run assigned files simultaneously |
| Correct primality | Deterministic Miller-Rabin with 12 witnesses — correct for all `uint64` |
| Worker failover | Coordinator reassigns a dead worker's file to any surviving worker |
| Cross-file dedup | Coordinator maintains a global `seen` map — duplicates across files never appear in output |
| Chandy-Lamport snapshots | Coordinator checkpoints to AFS after every file — resumes on crash without reprocessing completed work |
| AFS integration | Workers are AFS clients — files fetched, cached locally, reads are pure local I/O |

### Tasks implemented

| Task | Feature |
|------|---------|
| **4** | Coordinator-worker architecture, file discovery, round-robin assignment, parallel dispatch, worker failover, cross-file dedup, output written to AFS |
| **5** | Chandy-Lamport consistent snapshots on AFS, coordinator resume from snapshot, per-worker snapshot, HTTP `/snapshot` endpoint on each worker |

### Quick start

```bash
cd primefinder
go build -o bin/coordinator ./cmd/coordinator
go build -o bin/worker      ./cmd/worker

# Start workers
./bin/worker -id w1 -port 6001 &
./bin/worker -id w2 -port 6002 &
./bin/worker -id w3 -port 6003 &

# Run coordinator
./bin/coordinator \
  -afs localhost:50051,localhost:50052,localhost:50053 \
  -workers localhost:6001,localhost:6002,localhost:6003 \
  -cacheDir /tmp/coord-cache \
  -output primes.txt
```

---

## Running with Docker (recommended)

The entire platform runs with a single `docker compose` command. All services are on one Docker bridge network — containers discover each other by hostname.

```bash
# Build everything
docker compose build

# Start AFS servers + workers
docker compose up -d s1 s2 s3 worker1 worker2 worker3

# Wait for all 6 services to show healthy
docker compose ps

# Run the coordinator — discovers files, assigns to workers, writes primes.txt
docker compose --profile run run --rm coordinator

# Verify output on all 3 AFS servers
docker exec s1 cat /data/output/primes.txt
docker exec s2 cat /data/output/primes.txt
docker exec s3 cat /data/output/primes.txt

# All 3 must be identical
diff <(docker exec s1 cat /data/output/primes.txt) \
     <(docker exec s2 cat /data/output/primes.txt) && echo "s1 == s2"
diff <(docker exec s1 cat /data/output/primes.txt) \
     <(docker exec s3 cat /data/output/primes.txt) && echo "s1 == s3"
```

## Test suite

All testcases from the specification are implemented as Go programs in `primefinder/tests/`.
Full run instructions: **[primefinder/tests/README.md](./primefinder/README.md)**

| Test | What it proves |
|------|----------------|
| `test1a` | Single worker, single file — correct primes, no duplicates, no misses |
| `test1b` | Multiple workers, multiple files — correct merged output, cross-file dedup |
| `test2a` | Server crash during read — whole-file cache survives, new primary found |
| `test2b` | Client crash during write — partial write never reaches server |
| `test3` | Replication + failover + recovery — all 3 servers consistent throughout |
| `test4a` | Coordinator crash — resumes from Chandy-Lamport snapshot |
| `test4b` | Single worker failure — coordinator reassigns, output still correct |
| `test4c` | Majority worker failure — snapshot + surviving worker = correct output |
| `test5a` | Throughput scaling — speedup table for 1, 2, 3 workers |
| `test5b` | Large dataset — correctness and stability under load |


## Results for large Files optional test cases 

## | `test5a` | Throughput scaling — speedup table for 1, 2, 4,8 workers |

![Alt text](./scalling.png)



## | `test5b` | Throughput scaling — speedup table for 1, 2, 4,8 workers |

![Alt text](./LargeInputfiles.png)


---

## Project structure

```
finalproject/
├── docker-compose.yml              ← full platform: s1, s2, s3, worker1-3, coordinator
│
├── afsfs/                          ← Part 1: AFS Distributed Filesystem
│   ├── README.md                   ← full Part 1 documentation
│   ├── cmd/
│   │   ├── server/main.go          ← server binary
│   │   └── client/main.go          ← demo client
│   ├── pkg/
│   │   ├── server/
│   │   │   ├── handler.go          ← Open/Store/Create/Delete/Replicate/SyncState
│   │   │   ├── election.go         ← bully election + GetPrimary + AnnouncePrimary
│   │   │   ├── heartbeat.go        ← heartbeat sender (primary) + monitor (backup)
│   │   │   └── replication.go      ← replicateToAll + peer connections
│   │   └── afs/
│   │       ├── client.go           ← Open/Read/Write/Close/Create/Delete + failover
│   │       └── cache.go            ← local cache manager + version tracking
│   ├── proto/afs.proto             ← gRPC service definition
│   ├── generated/afs/              ← generated gRPC stubs (do not edit)
│   ├── tests/                      ← AFS test programs (test1a–test3c)
│   ├── testdata/
│   │   ├── input/                  ← input_dataset_001.txt, _002.txt, _003.txt
│   │   ├── output-s1/              ← s1 persistent storage
│   │   ├── output-s2/              ← s2 persistent storage
│   │   └── output-s3/              ← s3 persistent storage
│   └── Dockerfile
│
├── primefinder/                    ← Part 2: Distributed Prime Finder
│   ├── README.md                   ← full Part 2 documentation
│   ├── cmd/
│   │   ├── coordinator/main.go     ← coordinator binary
│   │   └── worker/main.go          ← worker binary + HTTP snapshot endpoint
│   ├── pkg/
│   │   ├── coordinator/
│   │   │   └── coordinator.go      ← full pipeline: discover→assign→dispatch→dedup→write
│   │   ├── prime/
│   │   │   ├── miller_rabin.go     ← IsPrime, mulmod, addmod, modpow
│   │   │   └── miller_rabin_test.go
│   │   └── snapshot/
│   │       └── snapshot.go         ← WorkerSnapshot, CoordSnapshot, save/load on AFS
│   ├── proto/prime.proto           ← WorkerService: ProcessFile + Health
│   ├── generated/prime/            ← generated gRPC stubs (do not edit)
│   ├── tests/
│   │   ├── README.md               ← full test run instructions
│   │   ├── test1a/ test1b/         ← basic functionality tests
│   │   ├── test2a/ test2b/         ← file server fault tolerance tests
│   │   ├── test3/                  ← replication + failover + recovery
│   │   ├── test4a/ test4b/ test4c/ ← prime finder fault tolerance tests
│   │   └── test5a/ test5b/         ← scaling and large dataset tests
│   └── Dockerfile
```

---

## Technology

| Component | Technology |
|-----------|------------|
| Language | Go |
| RPC framework | gRPC + protobuf |
| Containerization | Docker + Docker Compose |
| Primality algorithm | Deterministic Miller-Rabin |
| Snapshot algorithm | Chandy-Lamport (simplified for synchronous RPC topology) |
| Replication | Primary-backup with synchronous parallel replication |
| Leader election | Bully algorithm — lowest server ID wins |
| Failure detection | Heartbeat-based — 1s interval, 3s timeout |

---

## Design principle

Every design decision in this project follows the same principle:

> **Simplest correct design for this specific workload.**

The workload has three properties that justify every simplification made:

- Input files are **static** — whole-file caching always valid, `TestAuth` always returns valid after first fetch, no cache invalidation complexity needed
- Output file is **write-once** — dirty flag sufficient, no concurrent write conflicts, primary-backup sufficient without Raft
- **Lab scale** (3 servers) — lowest-ID election instead of voting, client holds server list, 3 satisfies the 2f+1 rule for f=1 failure tolerance

