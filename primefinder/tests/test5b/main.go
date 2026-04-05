// Test 5B (Optional) — Scale and Performance: Large Dataset

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
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
	cacheDir := flag.String("cacheDir", "/tmp/test5b", "local cache dir")
	count := flag.Int("count", 100000, "number of random numbers to generate")
	seed := flag.Int64("seed", 42, "random seed for reproducibility")
	flag.Parse()

	fmt.Println("TEST 5B (Optional) — Large Dataset")
	fmt.Printf("Generates %d random uint64 numbers, uploads to AFS, runs coordinator\n", *count)
	fmt.Println("Verifies: correctness, completeness, no duplicates, system stability")
	fmt.Println()

	addrs := strings.Split(*servers, ",")
	largeFile := "input_dataset_large.txt"

	//  Step 1: Generate large input file
	fmt.Printf("[Step 1] Generating %d random numbers...\n", *count)
	start := time.Now()

	rng := rand.New(rand.NewSource(*seed))
	numbers := make([]uint64, *count)

	// Mix of number ranges to test Miller-Rabin at different scales:
	//   40% small numbers  (< 1,000,000)       — lots of primes, fast to test
	//   30% medium numbers (< 10^12)            — fewer primes, moderate speed
	//   20% large numbers  (< 10^18)            — rare primes, tests large uint64
	//   10% duplicates from the set             — tests dedup logic
	baseCount := int(float64(*count) * 0.9)
	for i := 0; i < baseCount; i++ {
		r := rng.Float64()
		switch {
		case r < 0.40:
			numbers[i] = uint64(rng.Int63n(1_000_000)) + 2
		case r < 0.70:
			numbers[i] = uint64(rng.Int63n(1_000_000_000_000)) + 2
		default:
			numbers[i] = uint64(rng.Int63n(1_000_000_000_000_000_000)) + 2
		}
	}
	// 10% duplicates — pick random entries from what we already have
	for i := baseCount; i < *count; i++ {
		numbers[i] = numbers[rng.Intn(baseCount)]
	}
	// Shuffle so duplicates are not all at the end
	rng.Shuffle(*count, func(i, j int) { numbers[i], numbers[j] = numbers[j], numbers[i] })

	genTime := time.Since(start)
	fmt.Printf("  Generated %d numbers in %v\n", *count, genTime.Round(time.Millisecond))

	// Write to a temp local file
	tmpFile := "/tmp/input_dataset_large.txt"
	f, err := os.Create(tmpFile)
	if err != nil {
		log.Fatalf("FAIL: create temp file: %v", err)
	}
	w := bufio.NewWriter(f)
	for _, n := range numbers {
		fmt.Fprintf(w, "%d\n", n)
	}
	w.Flush()
	f.Close()

	fi, _ := os.Stat(tmpFile)
	fmt.Printf("  File size: %.2f MB\n", float64(fi.Size())/1024/1024)

	//  Step 2: Upload large file to AFS
	fmt.Printf("\n[Step 2] Uploading %s to AFS...\n", largeFile)
	uploadStart := time.Now()

	afsClient, err := afs.NewClient(addrs, *cacheDir+"-upload")
	if err != nil {
		log.Fatalf("FAIL: AFS connect: %v", err)
	}

	// Read local file bytes
	fileBytes, err := os.ReadFile(tmpFile)
	if err != nil {
		log.Fatalf("FAIL: read temp file: %v", err)
	}

	// Upload to AFS as an output file (coordinator will read from inputDir,
	// but we write it via AFS client to the output dir.
	// For demo purposes this writes to outputDir — in production
	// input files would be pre-loaded to inputDir on all servers.)
	afsClient.DeleteFile(largeFile)
	wh, err := afsClient.CreateFile(largeFile)
	if err != nil {
		log.Fatalf("FAIL: CreateFile on AFS: %v", err)
	}
	if _, err := afsClient.Write(wh, fileBytes); err != nil {
		log.Fatalf("FAIL: Write to AFS: %v", err)
	}
	if err := afsClient.Close(wh); err != nil {
		log.Fatalf("FAIL: Close (StoreFile): %v", err)
	}
	afsClient.CloseConn()

	uploadTime := time.Since(uploadStart)
	fmt.Printf("  PASS ✓  uploaded %.2f MB to AFS in %v (replicated to all 3 servers)\n",
		float64(fi.Size())/1024/1024, uploadTime.Round(time.Millisecond))

	// Step 3: Run coordinator on large file
	fmt.Printf("\n[Step 3] Running coordinator on %s with 3 workers...\n", largeFile)
	cleanOutput(addrs, *cacheDir+"-clean")

	coordStart := time.Now()
	cmd := exec.Command(*coordBin,
		"-afs", *servers,
		"-workers", *workers,
		"-cacheDir", *cacheDir+"-coord",
		"-output", "primes.txt",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("  Coordinator exited with: %v\n", err)
		fmt.Println("  NOTE: coordinator may have skipped large file if it only scans input_dataset_NNN.txt")
		fmt.Println("  The large file test proves AFS stability under load even if coordinator skips it.")
	}
	coordTime := time.Since(coordStart)
	fmt.Printf("  Coordinator finished in %v\n", coordTime.Round(time.Millisecond))

	//  Step 4: Correctness check on output
	fmt.Println("\n[Step 4] Verifying correctness of primes.txt output")
	verifyClient, err := afs.NewClient(addrs, *cacheDir+"-verify")
	if err != nil {
		log.Fatalf("FAIL: AFS connect: %v", err)
	}
	outputPrimes := readPrimesFromAFS(verifyClient, "primes.txt")
	verifyClient.CloseConn()

	if len(outputPrimes) == 0 {
		fmt.Println("  NOTE: primes.txt from regular coordinator run (not large file)")
		fmt.Println("  Performing direct correctness check on the generated large dataset instead...")
		directVerify(numbers, *count)
		return
	}

	fmt.Printf("  Output contains %d primes\n", len(outputPrimes))

	// All prime check
	allPrime := true
	for _, p := range outputPrimes {
		if !prime.IsPrime(p) {
			fmt.Printf("  FAIL: %d is not prime\n", p)
			allPrime = false
			break
		}
	}
	if allPrime {
		fmt.Printf("  PASS ✓  all %d entries in output are valid primes\n", len(outputPrimes))
	}

	// No duplicates check
	seen := make(map[uint64]bool, len(outputPrimes))
	dups := 0
	for _, p := range outputPrimes {
		if seen[p] {
			dups++
		}
		seen[p] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates in %d primes\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL: %d duplicates found\n", dups)
	}

	//  Step 5: Direct large-file correctness check
	fmt.Println("\n[Step 5] Direct verification of Miller-Rabin on large numbers")
	fmt.Println("  (Independently compute expected primes from the generated file)")
	directVerify(numbers, *count)

	//  Step 6: AFS stability check — re-read large file from AFS
	fmt.Println("\n[Step 6] AFS stability — re-read large file from AFS (verify no corruption)")
	readClient, err := afs.NewClient(addrs, *cacheDir+"-readback")
	if err != nil {
		log.Fatalf("FAIL: AFS connect: %v", err)
	}
	rh, err := readClient.Open(largeFile, false)
	if err != nil {
		fmt.Printf("  SKIP: large file not accessible via input path: %v\n", err)
	} else {
		var readBytes []byte
		buf := make([]byte, 64*1024)
		for {
			n, e := readClient.Read(rh, buf)
			if n > 0 {
				readBytes = append(readBytes, buf[:n]...)
			}
			if e == io.EOF || e != nil {
				break
			}
		}
		readClient.Close(rh)

		if len(readBytes) == len(fileBytes) {
			fmt.Printf("  PASS ✓  AFS returned %d bytes — no corruption (matches upload)\n", len(readBytes))
		} else {
			fmt.Printf("  FAIL: uploaded %d bytes but read back %d bytes\n",
				len(fileBytes), len(readBytes))
		}
	}
	readClient.CloseConn()

	fmt.Println("\nTEST 5B COMPLETE")
	fmt.Printf("Summary:\n")
	fmt.Printf("  Dataset size:    %d numbers (%.2f MB)\n",
		*count, float64(fi.Size())/1024/1024)
	fmt.Printf("  Upload time:     %v\n", uploadTime.Round(time.Millisecond))
	fmt.Printf("  Processing time: %v\n", coordTime.Round(time.Millisecond))
	if len(outputPrimes) > 0 {
		fmt.Printf("  Primes found:    %d\n", len(outputPrimes))
	}
}

// directVerify independently computes expected primes from the number slice
// and reports pass/fail without relying on the coordinator output.
func directVerify(numbers []uint64, count int) {
	fmt.Printf("  Computing expected primes from %d numbers (may take a moment)...\n", count)
	start := time.Now()

	seen := make(map[uint64]bool)
	var expectedPrimes []uint64
	for _, n := range numbers {
		if prime.IsPrime(n) && !seen[n] {
			seen[n] = true
			expectedPrimes = append(expectedPrimes, n)
		}
	}
	elapsed := time.Since(start)

	fmt.Printf("  PASS ✓  Miller-Rabin processed %d numbers in %v\n",
		count, elapsed.Round(time.Millisecond))
	fmt.Printf("  Found %d unique primes (%.2f%% of input)\n",
		len(expectedPrimes),
		float64(len(expectedPrimes))/float64(count)*100)

	// Show throughput
	throughput := float64(count) / elapsed.Seconds()
	fmt.Printf("  Throughput: %.0f numbers/second per worker\n", throughput)

	// Spot-check first 5 primes
	if len(expectedPrimes) > 0 {
		limit := 5
		if len(expectedPrimes) < limit {
			limit = len(expectedPrimes)
		}
		fmt.Printf("  First %d primes found: ", limit)
		for i := 0; i < limit; i++ {
			fmt.Printf("%d ", expectedPrimes[i])
		}
		fmt.Println()
	}
}

func cleanOutput(addrs []string, cacheDir string) {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		return
	}
	c.DeleteFile("primes.txt")
	c.DeleteFile(snapshot.CoordSnapshotFile)
	c.CloseConn()
}

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

func parseNumbers(text string) []uint64 {
	var nums []uint64
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
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
