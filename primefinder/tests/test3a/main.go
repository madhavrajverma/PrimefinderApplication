// Test 3A — File Server Replication
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	"afsfs/pkg/afs"
)

func main() {
	servers := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/test3a-cache", "local cache dir")
	flag.Parse()

	fmt.Println("TEST 3A — Replication")
	fmt.Println("Proves: write via primary replicates to ALL 3 servers before returning success")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	// Write via primary
	fmt.Println("[Step 1] Connect to AFS and write test3a_replication.txt via primary (s1)")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect: %v", err)
	}

	payload := "REPLICATION TEST — this line must appear on ALL 3 servers\n"
	client.DeleteFile("test3a_replication.txt")
	wh, err := client.CreateFile("test3a_replication.txt")
	if err != nil {
		log.Fatalf("FAIL: CreateFile: %v", err)
	}
	client.Write(wh, []byte(payload))
	if err := client.Close(wh); err != nil {
		log.Fatalf("FAIL: Close/StoreFile: %v", err)
	}
	client.CloseConn()
	fmt.Println("  Write completed — StoreFile RPC fired, primary replicated to s2 and s3 in parallel")

	// Read directly from each server individually
	fmt.Println("\n[Step 2] Read test3a_replication.txt directly from each server")
	fmt.Println("  (connecting to each server one by one, not via primary)")

	allPass := true
	serverNames := []string{"s1 (primary)", "s2 (backup)", "s3 (backup)"}
	for i, addr := range addrs {
		c, err := afs.NewClient([]string{addr}, fmt.Sprintf("%s-verify-s%d", *cacheDir, i+1))
		if err != nil {
			fmt.Printf("  %s (%s): FAIL — cannot connect: %v\n", serverNames[i], addr, err)
			allPass = false
			continue
		}
		h, err := c.Open("test3a_replication.txt", false)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL — file not found: %v\n", serverNames[i], addr, err)
			c.CloseConn()
			allPass = false
			continue
		}
		got := readAll(c, h)
		c.Close(h)
		c.CloseConn()

		if string(got) == payload {
			fmt.Printf("  %s (%s): PASS ✓  content correct\n", serverNames[i], addr)
		} else {
			fmt.Printf("  %s (%s): FAIL  got %q\n", serverNames[i], addr, string(got))
			allPass = false
		}
	}

	fmt.Println()
	if allPass {
		fmt.Println("PASS ✓  all 3 servers have identical content — replication works")
		fmt.Println()
		fmt.Println("Next: run test3b to demonstrate server failure during write")
		fmt.Println("  docker compose run --rm tester go run tests/test3b/main.go ...")
	} else {
		fmt.Println("FAIL: replication did not reach all servers")
	}

	fmt.Println("TEST 3A COMPLETE")
	fmt.Println("\nManual verify:")
	fmt.Println("  docker exec s1 cat /data/output/test3a_replication.txt")
	fmt.Println("  docker exec s2 cat /data/output/test3a_replication.txt")
	fmt.Println("  docker exec s3 cat /data/output/test3a_replication.txt")
}

func readAll(c *afs.Client, h int64) []byte {
	buf := make([]byte, 4096)
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
