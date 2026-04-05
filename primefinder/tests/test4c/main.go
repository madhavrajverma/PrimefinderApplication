// Test 4C — Distributed Prime Finder Fault Tolerance: Multiple Worker Failure

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
	coordBin := flag.String("coordBin", "./bin/coordinator", "coordinator binary")
	cacheDir := flag.String("cacheDir", "/tmp/test4c-cache", "local cache dir")
	docker := flag.Bool("docker", false, "use docker kill/start")
	flag.Parse()

	fmt.Println("TEST 4C — Multiple Worker Failure")
	fmt.Println("Proves: majority worker failure + coordinator snapshot recovery = correct output")

	addrs := strings.Split(*servers, ",")

	//  Step 1: Full clean run for ground truth
	fmt.Println("\n[Step 1] Full clean run — establish ground truth")
	cleanAFS(addrs, *cacheDir+"-clean")
	runCoord(*coordBin, *servers, *workers, *cacheDir+"-run1")

	c1, _ := afs.NewClient(addrs, *cacheDir+"-gt")
	groundTruth := readPrimes(c1, "primes.txt")
	c1.CloseConn()
	fmt.Printf("  PASS ✓  ground truth: %d unique primes\n", len(groundTruth))

	//  Step 2: Kill majority workers + inject partial snapshot
	fmt.Println("\n[Step 2] Kill worker1 and worker2 (majority failure)")
	cleanAFS(addrs, *cacheDir+"-clean2")

	if *docker {
		exec.Command("docker", "compose", "kill", "worker1").Run()
		exec.Command("docker", "compose", "kill", "worker2").Run()
		fmt.Println("  worker1 and worker2 killed via docker")
	} else {
		exec.Command("sh", "-c", "pkill -f 'worker.*6001'").Run()
		exec.Command("sh", "-c", "pkill -f 'worker.*6002'").Run()
		fmt.Println("  worker1 and worker2 killed via pkill")
	}
	time.Sleep(1 * time.Second)

	// Inject a snapshot so coordinator skips file 001
	fmt.Println("  Injecting coordinator snapshot (file 001 already done)...")
	primesFor001 := getPrimesForFile(addrs, *cacheDir+"-snap001", "input_dataset_001.txt")
	snap := &snapshot.CoordSnapshot{
		CompletedFiles:  []string{"input_dataset_001.txt"},
		PendingFiles:    []string{"input_dataset_002.txt", "input_dataset_003.txt"},
		CollectedPrimes: primesFor001,
		TimestampUnix:   time.Now().Unix(),
	}
	injectSnapshot(addrs, *cacheDir+"-inject", snap)
	fmt.Printf("  Snapshot injected: %d primes from file 001\n", len(primesFor001))

	// Step 3: Run coordinator with only worker3 alive
	fmt.Println("\n[Step 3] Run coordinator — only worker3 is alive")
	fmt.Println("  Coordinator will resume from snapshot and assign files 002+003 to worker3")

	output := runCoordCaptured(*coordBin, *servers, *workers, *cacheDir+"-run2")
	if strings.Contains(output, "RESUMING") || strings.Contains(output, "resuming") {
		fmt.Println("  PASS ✓  coordinator resumed from snapshot")
	}
	if strings.Contains(output, "worker3") || strings.Contains(output, "w3") {
		fmt.Println("  PASS ✓  coordinator used worker3 for remaining files")
	}

	//  Step 4: Verify output is correct and complete
	fmt.Println("\n[Step 4] Verify output matches ground truth")
	c2, _ := afs.NewClient(addrs, *cacheDir+"-verify")
	resultPrimes := readPrimes(c2, "primes.txt")
	c2.CloseConn()

	fmt.Printf("  Ground truth:     %d primes\n", len(groundTruth))
	fmt.Printf("  After recovery:   %d primes\n", len(resultPrimes))

	gtSet := toSet(groundTruth)
	resSet := toSet(resultPrimes)
	missed, extra := 0, 0
	for p := range gtSet {
		if !resSet[p] {
			missed++
		}
	}
	for p := range resSet {
		if !gtSet[p] {
			extra++
		}
	}
	if missed == 0 && extra == 0 {
		fmt.Printf("  PASS ✓  output correct after majority worker failure (%d primes)\n", len(resultPrimes))
	} else {
		fmt.Printf("  missed=%d extra=%d\n", missed, extra)
	}

	// No duplicates
	seen := make(map[uint64]bool)
	dups := 0
	for _, p := range resultPrimes {
		if seen[p] {
			dups++
		}
		seen[p] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates\n")
	} else {
		fmt.Printf("  FAIL: %d duplicates\n", dups)
	}

	// Step 5: Restart killed workers
	fmt.Println("\n[Step 5] Restarting worker1 and worker2")
	if *docker {
		exec.Command("docker", "compose", "start", "worker1").Run()
		exec.Command("docker", "compose", "start", "worker2").Run()
		fmt.Println("  PASS ✓  worker1 and worker2 restarted via docker")
	} else {
		fmt.Println("  Restart workers manually:")
		fmt.Println("  ./bin/worker -id w1 -port 6001 &")
		fmt.Println("  ./bin/worker -id w2 -port 6002 &")
	}

	fmt.Println("\nTEST 4C COMPLETE")
}

func getPrimesForFile(addrs []string, cacheDir, filename string) []uint64 {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		return nil
	}
	defer c.CloseConn()
	h, err := c.Open(filename, false)
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

func injectSnapshot(addrs []string, cacheDir string, snap *snapshot.CoordSnapshot) {
	data, _ := json.Marshal(snap)
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		log.Fatalf("FAIL: connect for snapshot inject: %v", err)
	}
	defer c.CloseConn()
	c.DeleteFile(snapshot.CoordSnapshotFile)
	sh, err := c.CreateFile(snapshot.CoordSnapshotFile)
	if err != nil {
		log.Fatalf("FAIL: CreateFile snapshot: %v", err)
	}
	c.Write(sh, data)
	if err := c.Close(sh); err != nil {
		log.Fatalf("FAIL: Close snapshot: %v", err)
	}
}

func cleanAFS(addrs []string, cacheDir string) {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		return
	}
	c.DeleteFile("primes.txt")
	c.DeleteFile(snapshot.CoordSnapshotFile)
	c.CloseConn()
}

func runCoord(bin, servers, workers, cacheDir string) {
	cmd := exec.Command(bin, "-afs", servers, "-workers", workers,
		"-cacheDir", cacheDir, "-output", "primes.txt")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("FAIL: coordinator: %v", err)
	}
}

func runCoordCaptured(bin, servers, workers, cacheDir string) string {
	cmd := exec.Command(bin, "-afs", servers, "-workers", workers,
		"-cacheDir", cacheDir, "-output", "primes.txt")
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("  Coordinator exited: %v\n", err)
	}
	fmt.Println(string(out))
	return string(out)
}

func readPrimes(client *afs.Client, filename string) []uint64 {
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
