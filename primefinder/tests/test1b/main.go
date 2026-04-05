// Test 1B — Basic Functionality: Multiple Workers, Multiple Files

package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"afsfs/pkg/afs"
	"primefinder/pkg/prime"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/test1b-cache", "local cache dir")
	outputFile := flag.String("outputFile", "primes.txt", "merged output file on AFS")
	flag.Parse()

	fmt.Println("TEST 1B — Basic Functionality: Multiple Workers, Multiple Files")
	fmt.Println("Verifies: merged output exists, all prime, no cross-file duplicates, no misses")
	fmt.Println()

	addrs := strings.Split(*servers, ",")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect to AFS: %v", err)
	}
	defer client.CloseConn()

	//Check 1: merged output file exists on AFS
	fmt.Printf("[Check 1] Merged output '%s' exists on AFS\n", *outputFile)
	oh, err := client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL: cannot open '%s' — did the coordinator run with 3 workers? error: %v\n",
			*outputFile, err)
		return
	}
	outputData := readAll(client, oh)
	client.Close(oh)
	outputPrimes := parseNumbers(outputData)
	fmt.Printf("  PASS ✓  '%s' found on AFS — %d entries\n", *outputFile, len(outputPrimes))

	// Check 2: every output number is actually prime
	fmt.Printf("\n[Check 2] Every number in output is actually prime\n")
	notPrime := 0
	for _, n := range outputPrimes {
		if !prime.IsPrime(n) {
			fmt.Printf("  FAIL: %d is NOT prime\n", n)
			notPrime++
		}
	}
	if notPrime == 0 {
		fmt.Printf("  PASS ✓  all %d entries are prime\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL: %d non-prime numbers found\n", notPrime)
	}

	//Check 3: no duplicates — cross-file dedup must work
	fmt.Printf("\n[Check 3] No duplicate primes in merged output (cross-file dedup)\n")
	seen := make(map[uint64]bool)
	dups := 0
	for _, n := range outputPrimes {
		if seen[n] {
			fmt.Printf("  FAIL: duplicate %d\n", n)
			dups++
		}
		seen[n] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates in %d primes\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL: %d duplicates found — coordinator cross-file dedup is broken\n", dups)
	}

	outSet := make(map[uint64]bool, len(outputPrimes))
	for _, p := range outputPrimes {
		outSet[p] = true
	}

	//  Check 4: no misses from any input file
	fmt.Printf("\n[Check 4] Every prime from all input files appears in merged output\n")
	inputFiles := []string{
		"input_dataset_001.txt",
		"input_dataset_002.txt",
		"input_dataset_003.txt",
	}

	totalMissed := 0
	for _, fname := range inputFiles {
		ih, err := client.Open(fname, false)
		if err != nil {
			fmt.Printf("  %s: SKIP — cannot open: %v\n", fname, err)
			continue
		}
		inputData := readAll(client, ih)
		client.Close(ih)
		nums := parseNumbers(inputData)

		missed := 0
		for _, n := range nums {
			if prime.IsPrime(n) && !outSet[n] {
				fmt.Printf("  FAIL: prime %d from %s missing in output\n", n, fname)
				missed++
			}
		}
		if missed == 0 {
			fmt.Printf("  %s: PASS ✓  all primes present in output\n", fname)
		} else {
			fmt.Printf("  %s: FAIL  %d primes missing\n", fname, missed)
			totalMissed += missed
		}
	}

	if totalMissed == 0 {
		fmt.Println("\n  PASS ✓  all primes from all input files are in the merged output")
	} else {
		fmt.Printf("\n  FAIL: %d primes total missing across all files\n", totalMissed)
	}

	fmt.Println("TEST 1B COMPLETE")
}

func readAll(c *afs.Client, h int64) []byte {
	buf := make([]byte, 64*1024)
	var out []byte
	for {
		n, e := c.Read(h, buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if e == io.EOF || e != nil {
			break
		}
	}
	return out
}

func parseNumbers(data []byte) []uint64 {
	var nums []uint64
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		n, err := strconv.ParseUint(line, 10, 64)
		if err == nil {
			nums = append(nums, n)
		}
	}
	return nums
}
