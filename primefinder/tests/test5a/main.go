// Test 5A (Optional) — Scale and Performance: Throughput Scaling

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

type runResult struct {
	workers    int
	workerList string
	elapsed    time.Duration
	primeCount int
	correct    bool
	dupFree    bool
}

func main() {
	servers := flag.String("servers", "s1:50051,s2:50052,s3:50053", "AFS server addresses")
	w1 := flag.String("w1", "worker1:6001", "1-worker config")
	w2 := flag.String("w2", "worker1:6001,worker2:6001", "2-worker config")
	w4 := flag.String("w4", "worker1:6001,worker2:6001,worker3:6001,worker4:6001", "4-worker config")
	w8 := flag.String("w8", "worker1:6001,worker2:6001,worker3:6001,worker4:6001,worker5:6001,worker6:6001,worker7:6001,worker8:6001", "8-worker config")
	coordBin := flag.String("coordBin", "./bin/coordinator", "coordinator binary")
	cacheDir := flag.String("cacheDir", "/tmp/test5a", "base cache dir")
	flag.Parse()

	fmt.Println("TEST 5A — Throughput Scaling")
	fmt.Println("Compares processing time with 1, 2, 4, 8 workers on the same dataset")
	fmt.Println()

	addrs := strings.Split(*servers, ",")

	// Establish ground truth with 3-worker run first
	fmt.Println("Establishing ground truth (3-worker run)...")
	cleanAFS(addrs, *cacheDir+"-init")
	runCoord(*coordBin, *servers, *w8, *cacheDir+"-init-run")
	c := mustConnect(addrs, *cacheDir+"-gt")
	groundTruth := toSet(readPrimes(c, "primes.txt"))
	c.CloseConn()
	fmt.Printf("Ground truth: %d unique primes\n\n", len(groundTruth))

	// Run with each worker configuration
	configs := []struct {
		n       int
		workers string
		label   string
	}{
		{1, *w1, "1 worker "},
		{2, *w2, "2 workers"},
		{4, *w4, "4 workers"},
		{8, *w8, "8 workers"},
	}

	var results []runResult

	for _, cfg := range configs {
		fmt.Printf("Running with %s...\n", cfg.label)
		cleanAFS(addrs, *cacheDir+fmt.Sprintf("-clean%d", cfg.n))

		start := time.Now()
		err := runCoordTimed(*coordBin, *servers, cfg.workers,
			*cacheDir+fmt.Sprintf("-run%d", cfg.n))
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("  ERROR: coordinator failed: %v\n", err)
			results = append(results, runResult{
				workers: cfg.n, workerList: cfg.workers,
				elapsed: elapsed, correct: false,
			})
			continue
		}

		// Verify correctness
		cv := mustConnect(addrs, *cacheDir+fmt.Sprintf("-verify%d", cfg.n))
		primes := readPrimes(cv, "primes.txt")
		cv.CloseConn()

		resultSet := toSet(primes)
		correct := setsEqual(groundTruth, resultSet)
		dupFree := len(primes) == len(resultSet)

		results = append(results, runResult{
			workers:    cfg.n,
			workerList: cfg.workers,
			elapsed:    elapsed,
			primeCount: len(primes),
			correct:    correct,
			dupFree:    dupFree,
		})

		status := "PASS ✓"
		if !correct || !dupFree {
			status = "FAIL ✗"
		}
		fmt.Printf("  %s  %s  time=%-10v  primes=%d\n",
			status, cfg.label, elapsed.Round(time.Millisecond), len(primes))
	}

	// Print performance table
	fmt.Println()
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Println("  SCALING RESULTS")
	fmt.Println("════════════════════════════════════════════════════════")
	fmt.Printf("  %-12s  %-12s  %-10s  %-8s  %-8s\n",
		"Workers", "Time", "Speedup", "Primes", "Correct")
	fmt.Println("  ─────────────────────────────────────────────────────")

	baseTime := results[0].elapsed
	for _, r := range results {
		speedup := float64(baseTime) / float64(r.elapsed)
		correctStr := "PASS ✓"
		if !r.correct || !r.dupFree {
			correctStr = "FAIL ✗"
		}
		fmt.Printf("  %-12d  %-12v  %-10.2fx  %-8d  %s\n",
			r.workers,
			r.elapsed.Round(time.Millisecond),
			speedup,
			r.primeCount,
			correctStr,
		)
	}
	fmt.Println("════════════════════════════════════════════════════════")

	// Analysis
	fmt.Println()
	fmt.Println("ANALYSIS:")
	if len(results) >= 2 {
		speedup2 := float64(results[0].elapsed) / float64(results[1].elapsed)
		fmt.Printf("  1→2 workers speedup: %.2fx", speedup2)
		if speedup2 >= 1.3 {
			fmt.Println("  ✓ Good scaling")
		} else {
			fmt.Println("  (limited by file count or AFS overhead)")
		}
	}
	if len(results) >= 3 {
		speedup4 := float64(results[0].elapsed) / float64(results[2].elapsed)
		fmt.Printf("  1→4 workers speedup: %.2fx", speedup4)
		if speedup4 >= 1.5 {
			fmt.Println("  ✓ Good scaling")
		} else {
			fmt.Println("  (limited by file count or AFS overhead)")
		}
	}
	if len(results) >= 4 {
		speedup8 := float64(results[0].elapsed) / float64(results[3].elapsed)
		fmt.Printf("  1→8 workers speedup: %.2fx", speedup8)
		if speedup8 >= 2.0 {
			fmt.Println("  ✓ Good scaling")
		} else {
			fmt.Println("  (limited by file count or AFS overhead)")
		}
	}

	fmt.Println()
	fmt.Println("NOTE: Speedup is bounded by the number of input files.")
	fmt.Println("  With 3 files and 8 workers → only 3 workers get files → ideal = 3x.")
	fmt.Println("  With 3 files and 4 workers → one worker gets 2 files → ideal = 2x.")
	fmt.Println("  With 3 files and 2 workers → one worker gets 2 files → ideal = 1.5x.")
	fmt.Println("  AFS caching overhead and coordinator dispatch time reduce actual speedup.")

	fmt.Println("\nTEST 5A COMPLETE")
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
		"-afs", servers, "-workers", workers,
		"-cacheDir", cacheDir, "-output", "primes.txt")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("FAIL: coordinator: %v", err)
	}
}

func runCoordTimed(bin, servers, workers, cacheDir string) error {
	cmd := exec.Command(bin,
		"-afs", servers, "-workers", workers,
		"-cacheDir", cacheDir, "-output", "primes.txt")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func mustConnect(addrs []string, cacheDir string) *afs.Client {
	c, err := afs.NewClient(addrs, cacheDir)
	if err != nil {
		log.Fatalf("FAIL: AFS connect: %v", err)
	}
	return c
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

func setsEqual(a, b map[uint64]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
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
