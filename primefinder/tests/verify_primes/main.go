// verify_primes — Cross-file prime output verifier
//
// Run this after the coordinator finishes with multiple workers and files.
// Verifies primes.txt on AFS against ALL input files combined.
//
// What this checks:
//   (a) primes.txt exists on AFS
//   (b) every number in primes.txt is actually prime
//   (c) no duplicates in primes.txt
//   (d) every prime from ALL input files appears in primes.txt — no misses
//   (e) cross-file dedup: a prime in multiple input files appears only ONCE in output
//
// Usage (inside tester container):
//   docker compose run --rm tester go run tests/verify_primes/main.go \
//     -servers s1:50051,s2:50052,s3:50053 \
//     -cacheDir /tmp/verify-cache \
//     -output primes.txt
//
// The test auto-discovers all input_dataset_NNN.txt files on AFS.

/*

docker compose run --rm tester go run tests/verify_primes/main.go \
-servers s1:50051,s2:50052,s3:50053 \
-cacheDir /tmp/verify-cache \
-output primes.txt
*/

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
	servers := flag.String("servers", "s1:50051,s2:50052,s3:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/verify-cache", "local cache directory")
	outputFile := flag.String("output", "primes.txt", "coordinator output file on AFS")
	flag.Parse()

	fmt.Println("══════════════════════════════════════════")
	fmt.Println("  VERIFY PRIMES — Multi-file Output Check")
	fmt.Println("══════════════════════════════════════════")
	fmt.Println()

	client, err := afs.NewClient(strings.Split(*servers, ","), *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect to AFS: %v", err)
	}
	defer client.CloseConn()

	// ── Check 1: output file exists ──────────────────────────────────────
	fmt.Printf("[Check 1] Output file '%s' exists on AFS\n", *outputFile)
	oh, err := client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL ✗  cannot open '%s': %v\n", *outputFile, err)
		fmt.Println("  Did the coordinator finish successfully?")
		return
	}
	outputData := readAll(client, oh)
	client.Close(oh)
	outputPrimes := parseNumbers(outputData)
	fmt.Printf("  PASS ✓  '%s' found — %d entries\n", *outputFile, len(outputPrimes))

	if len(outputPrimes) == 0 {
		fmt.Println("  FAIL ✗  output file is empty")
		return
	}

	// ── Check 2: every output number is prime ────────────────────────────
	fmt.Printf("\n[Check 2] Every number in '%s' is actually prime\n", *outputFile)
	notPrime := 0
	for _, n := range outputPrimes {
		if !prime.IsPrime(n) {
			fmt.Printf("  FAIL ✗  %d is NOT prime but appears in output\n", n)
			notPrime++
			if notPrime >= 5 {
				fmt.Println("  ... (stopping after 5 failures)")
				break
			}
		}
	}
	if notPrime == 0 {
		fmt.Printf("  PASS ✓  all %d numbers verified prime (Miller-Rabin)\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL ✗  %d non-prime numbers found in output\n", notPrime)
	}

	// ── Check 3: no duplicates ────────────────────────────────────────────
	fmt.Printf("\n[Check 3] No duplicate primes in output\n")
	seen := make(map[uint64]bool, len(outputPrimes))
	dups := 0
	for _, n := range outputPrimes {
		if seen[n] {
			fmt.Printf("  FAIL ✗  duplicate: %d\n", n)
			dups++
			if dups >= 5 {
				fmt.Println("  ... (stopping after 5)")
				break
			}
		}
		seen[n] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates among %d primes\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL ✗  %d duplicates found\n", dups)
	}

	// ── Check 4: discover all input files and check no misses ────────────
	fmt.Printf("\n[Check 4] Discovering all input files on AFS\n")
	var inputFiles []string
	for i := 1; i <= 9999; i++ {
		name := fmt.Sprintf("input_dataset_%03d.txt", i)
		h, err := client.Open(name, false)
		if err != nil {
			break
		}
		client.Close(h)
		inputFiles = append(inputFiles, name)
	}
	fmt.Printf("  Found %d input files: %v\n", len(inputFiles), inputFiles)

	// ── Check 5: no misses — every prime from every input file is in output
	fmt.Printf("\n[Check 5] No misses — every prime from all input files in output\n")
	outSet := make(map[uint64]bool, len(outputPrimes))
	for _, p := range outputPrimes {
		outSet[p] = true
	}

	totalInputNumbers := 0
	totalInputPrimes := 0
	totalMissed := 0

	for _, fname := range inputFiles {
		ih, err := client.Open(fname, false)
		if err != nil {
			fmt.Printf("  SKIP: cannot open %s: %v\n", fname, err)
			continue
		}
		data := readAll(client, ih)
		client.Close(ih)
		numbers := parseNumbers(data)
		totalInputNumbers += len(numbers)

		fileMissed := 0
		for _, n := range numbers {
			if prime.IsPrime(n) {
				totalInputPrimes++
				if !outSet[n] {
					fmt.Printf("  FAIL ✗  prime %d in %s missing from output\n", n, fname)
					fileMissed++
					totalMissed++
					if fileMissed >= 3 {
						fmt.Printf("  ... (stopping after 3 per file)\n")
						break
					}
				}
			}
		}
		if fileMissed == 0 {
			fmt.Printf("  %s: PASS ✓\n", fname)
		}
	}

	if totalMissed == 0 {
		fmt.Printf("  PASS ✓  all %d primes from %d input numbers present in output\n",
			totalInputPrimes, totalInputNumbers)
	} else {
		fmt.Printf("  FAIL ✗  %d primes missing from output\n", totalMissed)
	}

	// ── Check 6: cross-file dedup — shared primes appear only once ────────
	fmt.Printf("\n[Check 6] Cross-file dedup — shared primes appear exactly once in output\n")
	if dups == 0 {
		fmt.Println("  PASS ✓  (guaranteed by Check 3 — no duplicates in output)")
	}

	// ── Summary ───────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("══════════════════════════════════════════")
	fmt.Printf("  Output primes    : %d\n", len(outputPrimes))
	fmt.Printf("  Input files      : %d\n", len(inputFiles))
	fmt.Printf("  Input numbers    : %d\n", totalInputNumbers)
	fmt.Printf("  Input primes     : %d\n", totalInputPrimes)
	fmt.Printf("  Not-prime errors : %d\n", notPrime)
	fmt.Printf("  Duplicate errors : %d\n", dups)
	fmt.Printf("  Missing errors   : %d\n", totalMissed)
	fmt.Println("══════════════════════════════════════════")
	if notPrime == 0 && dups == 0 && totalMissed == 0 {
		fmt.Println("  ALL CHECKS PASSED ✓")
	} else {
		fmt.Println("  SOME CHECKS FAILED ✗")
	}
	fmt.Println("══════════════════════════════════════════")
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
