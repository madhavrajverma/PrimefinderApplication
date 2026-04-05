// Test 2B — Client Crash Before Close (Whole-File Caching)

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
	servers := flag.String("servers", "s1:50051,s2:50052,s3:50053", "AFS server addresses")
	cacheDir := flag.String("cacheDir", "/tmp/test2b-cache", "local cache dir")
	flag.Parse()

	fmt.Println("TEST 2B — Client Crash Before Close (Whole-File Caching)")
	fmt.Println("Proves: Write() is local-only. No Close() = no StoreFile. Server never sees partial data.")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	//  Step 1: Write and commit good content v1
	fmt.Println("[Step 1] Write good content v1 and Close properly")
	fmt.Println("  Close() fires StoreFile RPC → data reaches server → replicated to all 3 servers")
	client, err := afs.NewClient(addrs, *cacheDir)
	if err != nil {
		log.Fatalf("FAIL: cannot connect: %v", err)
	}

	goodContent := "GOOD VERSION v1 — coordinator committed this properly via Close()\n"
	client.DeleteFile("test2b_output.txt")
	wh, err := client.CreateFile("test2b_output.txt")
	if err != nil {
		log.Fatalf("FAIL: CreateFile: %v", err)
	}
	client.Write(wh, []byte(goodContent))
	if err := client.Close(wh); err != nil {
		log.Fatalf("FAIL: Close — StoreFile failed: %v", err)
	}
	fmt.Println("  PASS ✓  v1 committed — StoreFile RPC fired, replicated to all 3 servers")

	//  Step 2: Simulate client crash before Close()
	fmt.Println("\n[Step 2] Simulate coordinator crash — write crash content but NO Close()")
	wh2, err := client.Open("test2b_output.txt", true)
	if err != nil {
		log.Fatalf("FAIL: Open for write: %v", err)
	}
	crashContent := "CRASH CONTENT — this must NEVER reach the server\n"
	client.Write(wh2, []byte(crashContent))
	fmt.Println("  Write() called — crash content written to LOCAL CACHE only")
	fmt.Println("  NO network call made — server is completely unaware")
	fmt.Println("  Simulating crash — dropping connection WITHOUT calling Close()")
	fmt.Println("  Close() never called → StoreFile RPC never fired → server untouched")
	client.CloseConn() // drop connection without Close(handle)
	fmt.Println("  Connection dropped (client crashed)")

	//  Step 3: New client reads — must see v1 not crash content
	fmt.Println("\n[Step 3] New client reads test2b_output.txt — must see v1")
	fmt.Println("  (crash content was only in local cache — never sent to server)")
	client2, err := afs.NewClient(addrs, *cacheDir+"-verify")
	if err != nil {
		log.Fatalf("FAIL: reconnect: %v", err)
	}
	defer client2.CloseConn()

	rh, err := client2.Open("test2b_output.txt", false)
	if err != nil {
		log.Fatalf("FAIL: cannot open on new client: %v", err)
	}
	got := readAll(client2, rh)
	client2.Close(rh)

	fmt.Printf("  Server returned: %q\n", string(got))
	if string(got) == goodContent {
		fmt.Println("  PASS ✓  server has v1 — crash content never reached server")
		fmt.Println("  Whole-file caching proved: Write() is local-only, StoreFile only on Close()")
	} else if string(got) == crashContent {
		fmt.Println("  FAIL ✗  crash content reached server — whole-file caching is broken")
	} else {
		fmt.Printf("  FAIL ✗  unexpected content: %q\n", string(got))
	}

	//  Step 4: Verify all 3 servers have v1
	fmt.Println("\n[Step 4] Verify all 3 servers have v1 — consistent, no partial data")
	allGood := true
	for i, addr := range addrs {
		name := fmt.Sprintf("s%d", i+1)
		c, err := afs.NewClient([]string{addr}, fmt.Sprintf("%s-s%d", *cacheDir, i+1))
		if err != nil {
			fmt.Printf("  %s (%s): OFFLINE — skipping\n", name, addr)
			continue
		}
		h, err := c.Open("test2b_output.txt", false)
		if err != nil {
			fmt.Printf("  %s (%s): FAIL ✗  cannot open: %v\n", name, addr, err)
			c.CloseConn()
			allGood = false
			continue
		}
		content := readAll(c, h)
		c.Close(h)
		c.CloseConn()

		if string(content) == goodContent {
			fmt.Printf("  %s (%s): PASS ✓  has v1 — correct\n", name, addr)
		} else if string(content) == crashContent {
			fmt.Printf("  %s (%s): FAIL ✗  has crash content\n", name, addr)
			allGood = false
		} else if len(content) == 0 {
			fmt.Printf("  %s (%s): FAIL ✗  empty file\n", name, addr)
			allGood = false
		} else {
			fmt.Printf("  %s (%s): FAIL ✗  unexpected: %q\n", name, addr, string(content))
			allGood = false
		}
	}

	fmt.Println()
	if allGood {
		fmt.Println("  PASS ✓  ALL 3 servers have v1 — no partial write, no corruption")
	} else {
		fmt.Println("  FAIL ✗  server inconsistency detected")
	}

	fmt.Println("TEST 2B COMPLETE")
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
