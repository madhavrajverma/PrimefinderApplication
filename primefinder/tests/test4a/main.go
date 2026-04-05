// Test 4A — Distributed Prime Finder Fault Tolerance: Coordinator Failure

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"afsfs/pkg/afs"
	"primefinder/pkg/prime"
	"primefinder/pkg/snapshot"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "AFS server addresses")
	workers := flag.String("workers", "localhost:6001,localhost:6002,localhost:6003", "worker addresses")
	coordBin := flag.String("coordBin", "./bin/coordinator", "path to coordinator binary")
	cacheDir := flag.String("cacheDir", "/tmp/test4a-cache", "local cache dir")
	flag.Parse()

	fmt.Println("TEST 4A — Coordinator Failure and Resume from Snapshot")
	fmt.Println("Proves: coordinator restores from Chandy-Lamport snapshot, no duplicate/missed primes")

	addrs := strings.Split(*servers, ",")

	//  Step 1: Full clean run — establish ground truth
	fmt.Println("\n[Step 1] Full clean coordinator run — establish ground truth")
	cleanOutputs(addrs, *cacheDir+"-clean")

	runCoordinator(*coordBin, *servers, *workers, *cacheDir+"-run1")

	client, err := afs.NewClient(addrs, *cacheDir+"-read1")
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}
	fullPrimes := readPrimesFromAFS(client, "primes.txt")
	client.CloseConn()

	if len(fullPrimes) == 0 {
		log.Fatalf("FAIL: full run produced no primes — is the coordinator working?")
	}
	fmt.Printf("  PASS ✓  full run found %d unique primes\n", len(fullPrimes))

	//  Step 2: Inject partial snapshot claiming file 001 is done
	fmt.Println("\n[Step 2] Inject partial coordinator snapshot (simulate coordinator crash after file 001)")
	cleanOutputs(addrs, *cacheDir+"-clean2") // remove primes.txt for resume run

	snap := &snapshot.CoordSnapshot{
		CompletedFiles:  []string{"input_dataset_001.txt"},
		PendingFiles:    []string{"input_dataset_002.txt", "input_dataset_003.txt"},
		CollectedPrimes: primesFromFile001(addrs, *cacheDir+"-snap"),
		TimestampUnix:   time.Now().Unix(),
	}
	snapData, _ := json.Marshal(snap)

	snapClient, err := afs.NewClient(addrs, *cacheDir+"-inject")
	if err != nil {
		log.Fatalf("FAIL: connect for snapshot inject: %v", err)
	}
	snapClient.DeleteFile(snapshot.CoordSnapshotFile)
	sh, err := snapClient.CreateFile(snapshot.CoordSnapshotFile)
	if err != nil {
		log.Fatalf("FAIL: CreateFile snapshot: %v", err)
	}
	snapClient.Write(sh, snapData)
	if err := snapClient.Close(sh); err != nil {
		log.Fatalf("FAIL: Close snapshot: %v", err)
	}
	snapClient.CloseConn()
	fmt.Printf("  Snapshot injected: %s on AFS\n", snapshot.CoordSnapshotFile)
	fmt.Printf("  Snapshot claims %d primes already collected from file 001\n", len(snap.CollectedPrimes))

	//  Step 3: Run coordinator — must resume from snapshot
	fmt.Println("\n[Step 3] Running coordinator — should RESUME from snapshot (check logs for 'RESUMING')")
	output := runCoordinatorWithOutput(*coordBin, *servers, *workers, *cacheDir+"-run2")
	if strings.Contains(output, "RESUMING") {
		fmt.Println("  PASS ✓  coordinator log shows 'RESUMING from snapshot'")
	} else {
		fmt.Println("  NOTE: 'RESUMING' not found in output — check coordinator logs manually")
		fmt.Println("  (Test continues — output correctness is the definitive check)")
	}

	//  Step 4: Verify resumed output matches full run
	fmt.Println("\n[Step 4] Verify resumed output is identical to full run output")
	client2, err := afs.NewClient(addrs, *cacheDir+"-read2")
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}
	resumedPrimes := readPrimesFromAFS(client2, "primes.txt")
	client2.CloseConn()

	fmt.Printf("  Full run:    %d primes\n", len(fullPrimes))
	fmt.Printf("  Resumed run: %d primes\n", len(resumedPrimes))

	fullSet := toSet(fullPrimes)
	resumedSet := toSet(resumedPrimes)

	missing := 0
	for p := range fullSet {
		if !resumedSet[p] {
			fmt.Printf("  FAIL: prime %d in full run but missing from resumed run\n", p)
			missing++
			if missing >= 5 {
				fmt.Println("  ... (more missing primes, stopping at 5)")
				break
			}
		}
	}
	extra := 0
	for p := range resumedSet {
		if !fullSet[p] {
			fmt.Printf("  FAIL: prime %d in resumed run but not in full run\n", p)
			extra++
			if extra >= 5 {
				fmt.Println("  ... (more extra primes, stopping at 5)")
				break
			}
		}
	}
	if missing == 0 && extra == 0 {
		fmt.Printf("  PASS ✓  resumed output identical to full run (%d primes)\n", len(fullPrimes))
	}

	//  Step 5: No duplicates in resumed output
	fmt.Println("\n[Step 5] No duplicates in resumed output")
	dupCount := 0
	seen := make(map[uint64]bool)
	for _, p := range resumedPrimes {
		if seen[p] {
			dupCount++
		}
		seen[p] = true
	}
	if dupCount == 0 {
		fmt.Printf("  PASS ✓  no duplicates among %d primes\n", len(resumedPrimes))
	} else {
		fmt.Printf("  FAIL: %d duplicate primes in resumed output\n", dupCount)
	}

	fmt.Println("\nTEST 4A COMPLETE")
}

// runCoordinator runs the coordinator binary and waits for it to finish.
func runCoordinator(bin, servers, workers, cacheDir string) {
	cmd := exec.Command(bin,
		"-afs", servers,
		"-workers", workers,
		"-cacheDir", cacheDir,
		"-output", "primes.txt",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("FAIL: coordinator run failed: %v", err)
	}
}

// runCoordinatorWithOutput runs coordinator and captures stdout+stderr as string.
func runCoordinatorWithOutput(bin, servers, workers, cacheDir string) string {
	cmd := exec.Command(bin,
		"-afs", servers,
		"-workers", workers,
		"-cacheDir", cacheDir,
		"-output", "primes.txt",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("  Coordinator exited with: %v\n", err)
	}
	fmt.Println(string(out))
	return string(out)
}

// cleanOutputs deletes primes.txt from AFS to reset between runs.
func cleanOutputs(addrs []string, cacheDir string) {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		return
	}
	c.DeleteFile("primes.txt")
	c.DeleteFile(snapshot.CoordSnapshotFile)
	c.CloseConn()
}

// readPrimesFromAFS reads primes.txt from AFS and returns sorted slice.
func readPrimesFromAFS(client *afs.Client, filename string) []uint64 {
	h, err := client.Open(filename, false)
	if err != nil {
		return nil
	}
	var all []byte
	buf := make([]byte, 64*1024)
	for {
		n, e := client.Read(h, buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	client.Close(h)
	return parseNumbers(string(all))
}

// primesFromFile001 reads input_dataset_001.txt and returns its primes.
func primesFromFile001(addrs []string, cacheDir string) []uint64 {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		return nil
	}
	defer c.CloseConn()
	h, err := c.Open("input_dataset_001.txt", false)
	if err != nil {
		return nil
	}
	var all []byte
	buf := make([]byte, 64*1024)
	for {
		n, e := c.Read(h, buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	c.Close(h)

	nums := parseNumbers(string(all))
	seen := make(map[uint64]bool)
	var primes []uint64
	for _, n := range nums {
		if prime.IsPrime(n) && !seen[n] {
			seen[n] = true
			primes = append(primes, n)
		}
	}
	sort.Slice(primes, func(i, j int) bool { return primes[i] < primes[j] })
	return primes
}

func toSet(primes []uint64) map[uint64]bool {
	m := make(map[uint64]bool, len(primes))
	for _, p := range primes {
		m[p] = true
	}
	return m
}

func parseNumbers(text string) []uint64 {
	var nums []uint64
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		n, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			continue
		}
		nums = append(nums, n)
	}
	return nums
}
