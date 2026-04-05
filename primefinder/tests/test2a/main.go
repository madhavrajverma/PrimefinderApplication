// Test 2A — Server Fault Tolerance: Primary Crash During Coordinator Run

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
	"time"

	"afsfs/pkg/afs"
	"primefinder/pkg/prime"
)

func main() {
	servers := flag.String("servers", "s1:50051,s2:50052,s3:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/test2a-cache", "local cache dir")
	outputFile := flag.String("outputFile", "primes.txt", "coordinator output file")
	flag.Parse()

	fmt.Println("TEST 2A — Primary Crash During Coordinator Run")
	fmt.Println("Proves: election works, coordinator recovers, output correct, s1 syncs on restart")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect to AFS: %v", err)
	}
	defer client.CloseConn()

	//  Check 1: primes.txt exists on new primary
	fmt.Printf("[Check 1] Output '%s' exists on AFS (written to new primary after s1 died)\n", *outputFile)
	oh, err := client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL ✗  cannot open '%s': %v\n", *outputFile, err)
		fmt.Println("  Did the coordinator finish? Did you restart s1?")
		return
	}
	outputData := readAll(client, oh)
	client.Close(oh)
	outputPrimes := parseNumbers(outputData)
	fmt.Printf("  PASS ✓  '%s' found on AFS — %d primes\n", *outputFile, len(outputPrimes))

	//  Check 2: all output numbers are prime
	fmt.Printf("\n[Check 2] Every number in output is actually prime\n")
	notPrime := 0
	for _, n := range outputPrimes {
		if !prime.IsPrime(n) {
			fmt.Printf("  FAIL ✗  %d is NOT prime\n", n)
			notPrime++
			if notPrime >= 5 {
				fmt.Println("  ... stopping after 5")
				break
			}
		}
	}
	if notPrime == 0 {
		fmt.Printf("  PASS ✓  all %d primes verified correct\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL ✗  %d non-prime numbers found\n", notPrime)
	}

	//  Check 3: no duplicates
	fmt.Printf("\n[Check 3] No duplicate primes in output\n")
	seen := make(map[uint64]bool, len(outputPrimes))
	dups := 0
	for _, n := range outputPrimes {
		if seen[n] {
			dups++
		}
		seen[n] = true
	}
	if dups == 0 {
		fmt.Printf("  PASS ✓  no duplicates among %d primes\n", len(outputPrimes))
	} else {
		fmt.Printf("  FAIL ✗  %d duplicates found\n", dups)
	}

	//  Check 4: s1 has primes.txt after restart via SyncState
	fmt.Printf("\n[Check 4] s1 has '%s' after restart (SyncState from new primary)\n", *outputFile)
	fmt.Println("  Connecting directly to s1 only...")
	fmt.Println("  Waiting 3s for SyncState to complete...")
	time.Sleep(3 * time.Second)

	s1Client, err := afs.NewClient([]string{addrs[0]}, *cacheDir+"-s1only")
	if err != nil {
		fmt.Printf("  FAIL ✗  cannot connect to s1: %v\n", err)
		fmt.Println("  Is s1 back up? Run: docker compose start s1 && sleep 5")
		return
	}
	defer s1Client.CloseConn()

	s1h, err := s1Client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL ✗  s1 does not have '%s' after restart: %v\n", *outputFile, err)
		fmt.Println("  SyncState did not replicate the file to s1")
		return
	}
	s1Data := readAll(s1Client, s1h)
	s1Client.Close(s1h)
	s1Primes := parseNumbers(s1Data)

	if len(s1Primes) == len(outputPrimes) {
		fmt.Printf("  PASS ✓  s1 synced '%s' — %d primes (matches new primary)\n",
			*outputFile, len(s1Primes))
	} else {
		fmt.Printf("  FAIL ✗  s1 has %d primes but new primary has %d — sync incomplete\n",
			len(s1Primes), len(outputPrimes))
	}

	//  Summary
	fmt.Println()

	if notPrime == 0 && dups == 0 && len(s1Primes) == len(outputPrimes) {
		fmt.Println("  TEST 2A PASSED ✓")
		fmt.Println("  • Election elected new primary")
		fmt.Println("  • Coordinator completed on new primary")
		fmt.Println("  • Output is correct and complete")
		fmt.Println("  • s1 synced all files on restart via SyncState")
	} else {
		fmt.Println("  TEST 2A FAILED ✗")
	}

	fmt.Println("\nManual verify on disk:")
	fmt.Println("  docker exec s1 ls -lh /data/output/")
	fmt.Println("  docker exec s2 ls -lh /data/output/")
	fmt.Println("  docker exec s3 ls -lh /data/output/")
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
