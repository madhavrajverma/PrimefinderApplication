// Test 1A — Basic Functionality: Single Worker, Single File

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
	cacheDir := flag.String("cacheDir", "/tmp/test1a-cache", "local cache dir")
	inputFile := flag.String("inputFile", "input_dataset_001.txt", "input file to verify against")
	outputFile := flag.String("outputFile", "primes.txt", "output file written by coordinator")
	flag.Parse()

	fmt.Println("TEST 1A — Basic Functionality: Single Worker, Single File")
	fmt.Println("Verifies: output exists on AFS, all entries prime, no duplicates, no misses")
	fmt.Println()

	addrs := strings.Split(*servers, ",")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect to AFS: %v", err)
	}
	defer client.CloseConn()

	//Check 1: output file exists on AFS
	fmt.Printf("[Check 1] Output file '%s' exists on AFS after coordinator closed it\n", *outputFile)
	oh, err := client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL: cannot open '%s' — did the coordinator run? error: %v\n", *outputFile, err)
		return
	}
	outputData := readAll(client, oh)
	client.Close(oh)
	outputPrimes := parseNumbers(outputData)
	fmt.Printf("  PASS ✓  '%s' found on AFS — %d entries\n", *outputFile, len(outputPrimes))

	//  Check 2: every output number is actually prime
	fmt.Printf("\n[Check 2] Every number in '%s' is actually prime\n", *outputFile)
	notPrime := 0
	for _, n := range outputPrimes {
		if !prime.IsPrime(n) {
			fmt.Printf("  FAIL: %d is NOT prime but appears in output\n", n)
			notPrime++
		}
	}
	if notPrime == 0 {
		fmt.Printf("  PASS ✓  all %d numbers in output are prime\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL: %d non-prime numbers in output\n", notPrime)
	}

	//  Check 3: no duplicates
	fmt.Printf("\n[Check 3] No duplicate primes in '%s'\n", *outputFile)
	seen := make(map[uint64]bool)
	dups := 0
	for _, n := range outputPrimes {
		if seen[n] {
			fmt.Printf("  FAIL: duplicate prime %d in output\n", n)
			dups++
		}
		seen[n] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates among %d primes\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL: %d duplicates found\n", dups)
	}

	//  Check 4: no misses — every prime from input is in output
	fmt.Printf("\n[Check 4] Every prime from '%s' appears in output (no misses)\n", *inputFile)
	ih, err := client.Open(*inputFile, false)
	if err != nil {
		fmt.Printf("  SKIP: cannot open %s: %v\n", *inputFile, err)
	} else {
		inputData := readAll(client, ih)
		client.Close(ih)
		inputNumbers := parseNumbers(inputData)

		outSet := make(map[uint64]bool, len(outputPrimes))
		for _, p := range outputPrimes {
			outSet[p] = true
		}

		missed := 0
		for _, n := range inputNumbers {
			if prime.IsPrime(n) && !outSet[n] {
				fmt.Printf("  FAIL: prime %d is in input but missing from output\n", n)
				missed++
			}
		}
		if missed == 0 {
			fmt.Printf("  PASS ✓  all primes from '%s' are in output\n", *inputFile)
		} else {
			fmt.Printf("  FAIL: %d primes missing from output\n", missed)
		}
	}

	fmt.Println("TEST 1A COMPLETE")
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
