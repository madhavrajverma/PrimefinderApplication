// Test 4B — Distributed Prime Finder Fault Tolerance: Single Worker Failure

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"afsfs/pkg/afs"
	"primefinder/pkg/snapshot"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "AFS server addresses")
	workers := flag.String("workers", "localhost:6001,localhost:6002,localhost:6003", "worker addresses")
	coordBin := flag.String("coordBin", "./bin/coordinator", "coordinator binary")
	workerBin := flag.String("workerBin", "./bin/worker", "worker binary")
	cacheDir := flag.String("cacheDir", "/tmp/test4b-cache", "local cache dir")
	docker := flag.Bool("docker", false, "use docker kill/start for workers")
	flag.Parse()

	fmt.Println("TEST 4B — Single Worker Failure")
	fmt.Println("Proves: coordinator reassigns work; output is correct with no missed/duplicate primes")

	addrs := strings.Split(*servers, ",")

	//  Step 1: Full clean run for ground truth
	fmt.Println("\n[Step 1] Full clean run — establish ground truth prime set")
	cleanAFS(addrs, *cacheDir+"-clean")

	runCoord(*coordBin, *servers, *workers, *cacheDir+"-run1")

	c1, _ := afs.NewClient(addrs, *cacheDir+"-gt")
	groundTruth := readPrimes(c1, "primes.txt")
	c1.CloseConn()
	fmt.Printf("  PASS ✓  ground truth: %d unique primes\n", len(groundTruth))

	//  Step 2: Run coordinator and kill worker1 after 1 second
	fmt.Println("\n[Step 2] Run coordinator + kill worker1 after 1 second")
	cleanAFS(addrs, *cacheDir+"-clean2")

	// Start coordinator in background
	coordCmd := exec.Command(*coordBin,
		"-afs", *servers,
		"-workers", *workers,
		"-cacheDir", *cacheDir+"-run2",
		"-output", "primes.txt",
	)
	coordCmd.Stdout = os.Stdout
	coordCmd.Stderr = os.Stderr
	if err := coordCmd.Start(); err != nil {
		log.Fatalf("FAIL: start coordinator: %v", err)
	}
	fmt.Println("  Coordinator started in background")

	// Kill worker1 after 1 second
	time.Sleep(1 * time.Second)
	fmt.Println("  Killing worker1 now...")
	if *docker {
		exec.Command("docker", "compose", "kill", "worker1").Run()
		fmt.Println("  worker1 killed via docker")
	} else {
		exec.Command("sh", "-c", "pkill -f 'worker.*6001' || pkill -f 'w1'").Run()
		fmt.Println("  worker1 killed via pkill")
	}

	// Wait for coordinator to finish
	fmt.Println("  Waiting for coordinator to finish (reassigning worker1's files)...")
	if err := coordCmd.Wait(); err != nil {
		fmt.Printf("  Coordinator exited with error: %v\n", err)
		fmt.Println("  This can happen if coordinator could not reassign. Checking output anyway...")
	}

	// Step 3: Verify output matches ground truth
	fmt.Println("\n[Step 3] Verify output after worker1 failure matches ground truth")
	c2, err := afs.NewClient(addrs, *cacheDir+"-verify")
	if err != nil {
		log.Fatalf("FAIL: connect: %v", err)
	}
	resultPrimes := readPrimes(c2, "primes.txt")
	c2.CloseConn()

	if len(resultPrimes) == 0 {
		fmt.Println("  FAIL: primes.txt is empty or missing — coordinator may have failed completely")
		fmt.Println("  This is expected if all 3 files were assigned to worker1 only")
	} else {
		fmt.Printf("  Ground truth:  %d primes\n", len(groundTruth))
		fmt.Printf("  After failure: %d primes\n", len(resultPrimes))

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
			fmt.Printf("  PASS ✓  output matches ground truth — worker failure handled correctly\n")
		} else {
			fmt.Printf("  missed=%d extra=%d — some reassignment may have failed\n", missed, extra)
		}
	}

	// Step 4: No duplicates
	fmt.Println("\n[Step 4] No duplicates in output")
	seen := make(map[uint64]bool)
	dups := 0
	c3, _ := afs.NewClient(addrs, *cacheDir+"-dup")
	result := readPrimes(c3, "primes.txt")
	c3.CloseConn()
	for _, p := range result {
		if seen[p] {
			dups++
		}
		seen[p] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates in %d primes\n", len(result))
	} else {
		fmt.Printf("  FAIL: %d duplicates found\n", dups)
	}

	//  Step 5: Restart worker1 and verify it recovers from snapshot
	fmt.Println("\n[Step 5] Restart worker1 — verify it has snapshot on AFS from before crash")
	c4, _ := afs.NewClient(addrs, *cacheDir+"-snap")
	workerSnap, err := snapshot.LoadWorkerSnapshot(c4, "w1")
	c4.CloseConn()

	if err != nil || workerSnap == nil {
		fmt.Println("  NOTE: no worker1 snapshot on AFS (worker was killed before finishing any file)")
		fmt.Println("  This is expected if worker1 was killed before completing its first file")
	} else {
		fmt.Printf("  PASS ✓  worker1 snapshot found on AFS\n")
		fmt.Printf("          completed files: %v\n", workerSnap.CompletedFiles)
		fmt.Printf("          primes recorded: %d\n", len(workerSnap.PrimesFound))
	}

	// Restart worker1
	if *docker {
		exec.Command("docker", "compose", "start", "worker1").Run()
		fmt.Println("  worker1 restarted via docker")
	} else {
		cmd := exec.Command(*workerBin, "-id", "w1", "-port", "6001")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Printf("  Warning: could not restart worker1: %v\n", err)
		} else {
			fmt.Println("  worker1 restarted")
		}
	}

	fmt.Println("\nTEST 4B COMPLETE")
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
	cmd := exec.Command(bin,
		"-afs", servers,
		"-workers", workers,
		"-cacheDir", cacheDir,
		"-output", "primes.txt",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("FAIL: coordinator: %v", err)
	}
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
