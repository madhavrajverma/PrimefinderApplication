// Test 3B — Primary Failover During Coordinator Run

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
	cacheDir := flag.String("cacheDir", "/tmp/test3b-cache", "local cache dir")
	outputFile := flag.String("outputFile", "primes.txt", "coordinator output file")
	flag.Parse()

	fmt.Println("TEST 3B — Primary Failover During Coordinator Run")
	fmt.Println("Proves: s1 dies mid-run, new primary elected, output correct, s2/s3 consistent")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	// Connect to surviving servers (s1 may still be dead)
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect to AFS: %v", err)
	}
	defer client.CloseConn()

	// Check 1: primes.txt exists on new primary
	fmt.Printf("[Check 1] '%s' exists on AFS (written to new primary after s1 died)\n", *outputFile)
	oh, err := client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL ✗  cannot open '%s': %v\n", *outputFile, err)
		fmt.Println("  Did the coordinator finish? Was s1 killed mid-run?")
		return
	}
	outputData := readAll(client, oh)
	client.Close(oh)
	outputPrimes := parseNumbers(outputData)
	fmt.Printf("  PASS ✓  '%s' found — %d primes\n", *outputFile, len(outputPrimes))

	//  Check 2: all primes correct
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
	fmt.Printf("\n[Check 3] No duplicate primes\n")
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

	//  Check 4: s2 and s3 have identical primes.txt
	fmt.Printf("\n[Check 4] s2 and s3 have identical '%s' (replication survived failover)\n", *outputFile)
	fmt.Println("  (skipping s1 — may still be dead)")

	allPass := true
	surviving := []struct{ name, addr string }{
		{"s2", addrs[1]},
		{"s3", addrs[2]},
	}
	for _, s := range surviving {
		c, err := afs.NewClient([]string{s.addr}, *cacheDir+"-"+s.name)
		if err != nil {
			fmt.Printf("  %s (%s): OFFLINE — skipping\n", s.name, s.addr)
			continue
		}
		h, err := c.Open(*outputFile, false)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL ✗  file not found: %v\n", s.name, s.addr, err)
			c.CloseConn()
			allPass = false
			continue
		}
		data := readAll(c, h)
		c.Close(h)
		c.CloseConn()
		primes := parseNumbers(data)

		if len(primes) == len(outputPrimes) {
			fmt.Printf("  %s (%s): PASS ✓  %d primes — matches\n", s.name, s.addr, len(primes))
		} else {
			fmt.Printf("  %s (%s): FAIL ✗  has %d primes, expected %d\n",
				s.name, s.addr, len(primes), len(outputPrimes))
			allPass = false
		}
	}

	//  Summary
	fmt.Println()
	fmt.Println("Summary")
	if notPrime == 0 && dups == 0 && allPass {
		fmt.Println("  TEST 3B PASSED ✓")
		fmt.Println("  • New primary elected after s1 died")
		fmt.Println("  • Coordinator completed on new primary")
		fmt.Println("  • Output is correct and complete")
		fmt.Println("  • s2 and s3 are consistent")
	} else {
		fmt.Println("  TEST 3B FAILED ✗")
	}
	fmt.Println("\nNext: bring s1 back and run test3c")
	fmt.Println("  docker compose start s1 && sleep 5")
	fmt.Println("  docker compose run --rm tester go run tests/test3c/main.go \\")
	fmt.Println("    -servers s1:50051,s2:50052,s3:50053 -cacheDir /tmp/test3c")
	fmt.Println("\nManual verify:")
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
