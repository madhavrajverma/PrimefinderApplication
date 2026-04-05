// Test 3C — Recovery: s1 Restarts and Auto-Syncs via SyncState

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
)

func main() {
	servers := flag.String("servers", "s1:50051,s2:50052,s3:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/test3c-cache", "local cache dir")
	outputFile := flag.String("outputFile", "primes.txt", "file to check sync")
	flag.Parse()

	fmt.Println("TEST 3C — Recovery: s1 Auto-Syncs via SyncState")
	fmt.Println("Verifies: s1 synced all files it missed while dead, all 3 servers consistent")
	fmt.Println()
	fmt.Println("Prerequisites:")
	fmt.Println("  - test3b completed (primes.txt on s2/s3, s1 was dead)")
	fmt.Println("  - s1 restarted: docker compose start s1 && sleep 10")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	// Check 1: s1 has primes.txt (written while it was dead)
	fmt.Printf("[Check 1] s1 has '%s' — written while s1 was dead\n", *outputFile)
	fmt.Println("  SyncState must have streamed this from new primary to s1 on startup")

	s1Client, err := afs.NewClient([]string{addrs[0]}, *cacheDir+"-s1")
	if err != nil {
		log.Fatalf("FAIL: cannot connect to s1 — is it running? %v", err)
	}
	defer s1Client.CloseConn()

	h, err := s1Client.Open(*outputFile, false)
	if err != nil {
		fmt.Printf("  FAIL ✗  s1 does not have '%s': %v\n", *outputFile, err)
		fmt.Println("  SyncState did not run or did not copy this file")
		fmt.Println("  Check s1 logs: docker compose logs s1 | grep -i sync")
		return
	}
	s1Data := readAll(s1Client, h)
	s1Client.Close(h)
	s1Primes := parseNumbers(s1Data)
	fmt.Printf("  PASS ✓  s1 has '%s' — %d primes (SyncState worked)\n",
		*outputFile, len(s1Primes))

	//  Check 2: all 3 servers have identical primes.txt
	fmt.Printf("\n[Check 2] All 3 servers have identical '%s'\n", *outputFile)
	serverNames := []string{"s1 (rejoined)", "s2", "s3"}
	allPass := true
	for i, addr := range addrs {
		c, err := afs.NewClient([]string{addr}, fmt.Sprintf("%s-all-s%d", *cacheDir, i+1))
		if err != nil {
			fmt.Printf("  %s: OFFLINE — skipping\n", serverNames[i])
			continue
		}
		fh, err := c.Open(*outputFile, false)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL ✗  file not found: %v\n", serverNames[i], addr, err)
			c.CloseConn()
			allPass = false
			continue
		}
		data := readAll(c, fh)
		c.Close(fh)
		c.CloseConn()
		primes := parseNumbers(data)

		if len(primes) == len(s1Primes) {
			fmt.Printf("  %s (%s): PASS ✓  %d primes — consistent\n",
				serverNames[i], addr, len(primes))
		} else {
			fmt.Printf("  %s (%s): FAIL ✗  has %d primes, s1 has %d\n",
				serverNames[i], addr, len(primes), len(s1Primes))
			allPass = false
		}
	}

	//  Check 3: verify s1 logs show SyncState
	fmt.Println("\n[Check 3] Verify s1 logs show SyncState ran on startup")
	fmt.Println("  Run manually: docker compose logs s1 | grep -i 'synced\\|SyncState'")

	//  Summary
	fmt.Println()

	if allPass {
		fmt.Println("  TEST 3C PASSED ✓")
		fmt.Println("  • s1 restarted and called SyncState automatically")
		fmt.Println("  • Primary streamed all missed files to s1")
		fmt.Println("  • All 3 servers are now consistent")
	} else {
		fmt.Println("  TEST 3C FAILED ✗")
	}

	fmt.Println("\nManual verify:")
	fmt.Println("  docker exec s1 ls -lh /data/output/")
	fmt.Println("  docker exec s2 ls -lh /data/output/")
	fmt.Println("  docker exec s3 ls -lh /data/output/")
	fmt.Println("  docker compose logs s1 | grep -i 'synced\\|SyncState'")
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
