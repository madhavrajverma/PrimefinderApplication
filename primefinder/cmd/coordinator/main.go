package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"primefinder/pkg/coordinator"
)

func main() {

	afsServers := flag.String("afs", "localhost:50051,localhost:50052,localhost:50053", "comma-separated AFS server addresses")
	workers := flag.String("workers", "localhost:6001,localhost:6002,localhost:6003", "comma-separated worker addresses")
	cacheDir := flag.String("cacheDir", "/tmp/prime-coord", "coordinator AFS cache dir")
	outputFile := flag.String("output", "primes.txt", "output file name on AFS")

	flag.Parse()

	cfg := coordinator.Config{
		AfsServers:  strings.Split(*afsServers, ","),
		WorkerAddrs: strings.Split(*workers, ","),
		CacheDir:    *cacheDir,
		OutputFile:  *outputFile,
	}

	log.Printf("coordinator starting")
	log.Printf("  AFS servers: %v", cfg.AfsServers)
	log.Printf("  Workers:     %v", cfg.WorkerAddrs)
	log.Printf("  Output:      %s", cfg.OutputFile)

	start := time.Now()
	if err := coordinator.Run(cfg); err != nil {
		log.Fatalf("coordinator failed: %v", err)
	}
	log.Printf("coordinator: DONE in %v", time.Since(start))
}
