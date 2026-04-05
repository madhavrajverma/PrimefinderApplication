package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	afs "afsfs/pkg/afs"
	pb "primefinder/generated/prime"
	"primefinder/pkg/prime"
	"primefinder/pkg/snapshot"
)

const primeBatchSize = 10000

type workerServer struct {
	pb.UnimplementedWorkerServiceServer
	workerID       string
	mu             sync.RWMutex
	completedFiles []string
	primesFound    []uint64
	seenPrimes     map[uint64]bool
}

func newWorkerServer(id string) *workerServer {
	return &workerServer{
		workerID:   id,
		seenPrimes: make(map[uint64]bool),
	}
}

func (w *workerServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{Alive: true, WorkerId: w.workerID}, nil
}

func (w *workerServer) ProcessFile(req *pb.ProcessFileRequest, stream pb.WorkerService_ProcessFileServer) error {
	start := time.Now()
	log.Printf("worker %s: processing %s", w.workerID, req.FilePath)

	servers := strings.Split(req.AfsServers, ",")
	client, err := afs.NewClient(servers, req.CacheDir)
	if err != nil {
		return stream.Send(&pb.ProcessFileResponse{
			Error: fmt.Sprintf("AFS connect failed: %v", err),
		})
	}
	defer client.CloseConn()

	handle, err := client.Open(req.FilePath, false)
	if err != nil {
		return stream.Send(&pb.ProcessFileResponse{
			Error: fmt.Sprintf("Open failed: %v", err),
		})
	}

	pr, pw := io.Pipe()

	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, readErr := client.Read(handle, buf)
			if n > 0 {
				if _, err := pw.Write(buf[:n]); err != nil {
					pw.CloseWithError(err)
					return
				}
			}
			if readErr == io.EOF {
				pw.Close()
				return
			}
			if readErr != nil {
				pw.CloseWithError(readErr)
				return
			}
		}
	}()

	fileSeenPrimes := make(map[uint64]bool)
	var batch []uint64
	count := int64(0)

	scanner := bufio.NewScanner(pr)
	scanner.Buffer(make([]byte, 256), 256)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		num, err := strconv.ParseUint(line, 10, 64)
		if err != nil {
			continue
		}
		count++

		if prime.IsPrime(num) && !fileSeenPrimes[num] {
			fileSeenPrimes[num] = true
			batch = append(batch, num)

			if len(batch) >= primeBatchSize {
				if err := stream.Send(&pb.ProcessFileResponse{Primes: batch}); err != nil {
					pr.CloseWithError(err)
					client.Close(handle)
					return err
				}
				batch = nil
			}
		}
	}

	client.Close(handle)

	if err := scanner.Err(); err != nil {
		return stream.Send(&pb.ProcessFileResponse{
			Error: fmt.Sprintf("scan error: %v", err),
		})
	}

	if len(batch) > 0 {
		if err := stream.Send(&pb.ProcessFileResponse{Primes: batch}); err != nil {
			return err
		}
	}

	elapsed := time.Since(start).Milliseconds()
	log.Printf("worker %s: found %d primes from %d numbers in %dms",
		w.workerID, len(fileSeenPrimes), count, elapsed)

	if err := stream.Send(&pb.ProcessFileResponse{
		Count:     count,
		ElapsedMs: elapsed,
	}); err != nil {
		return err
	}

	w.mu.Lock()
	for p := range fileSeenPrimes {
		if !w.seenPrimes[p] {
			w.seenPrimes[p] = true
			w.primesFound = append(w.primesFound, p)
		}
	}
	w.completedFiles = append(w.completedFiles, req.FilePath)
	snap := &snapshot.WorkerSnapshot{
		WorkerID:       w.workerID,
		CompletedFiles: append([]string{}, w.completedFiles...),
		PrimesFound:    append([]uint64{}, w.primesFound...),
		TimestampUnix:  time.Now().Unix(),
	}
	w.mu.Unlock()

	if saveErr := snapshot.SaveWorkerSnapshot(client, snap); saveErr != nil {
		log.Printf("worker %s: warning — could not save snapshot: %v", w.workerID, saveErr)
	}

	return nil
}

func (w *workerServer) currentSnapshot() *snapshot.WorkerSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return &snapshot.WorkerSnapshot{
		WorkerID:       w.workerID,
		CompletedFiles: append([]string{}, w.completedFiles...),
		PrimesFound:    append([]uint64{}, w.primesFound...),
		TimestampUnix:  time.Now().Unix(),
	}
}

func main() {
	port := flag.String("port", "6001", "gRPC port")
	workerID := flag.String("id", "w1", "worker id")
	snapshotPort := flag.String("snapshotPort", "6100", "HTTP snapshot port")
	flag.Parse()

	srv := newWorkerServer(*workerID)

	mux := http.NewServeMux()
	mux.HandleFunc("/snapshot", func(rw http.ResponseWriter, r *http.Request) {
		snap := srv.currentSnapshot()

		if r.Method == http.MethodPost {
			log.Printf("worker %s: [Chandy-Lamport] received snapshot marker — recording local state "+
				"(completed=%d files, primes=%d)",
				*workerID, len(snap.CompletedFiles), len(snap.PrimesFound))
		}

		rw.Header().Set("Content-Type", "application/json")
		json.NewEncoder(rw).Encode(snap)
	})
	mux.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		fmt.Fprintf(rw, "worker %s OK", *workerID)
	})

	go func() {
		log.Printf("worker %s: HTTP snapshot endpoint on :%s", *workerID, *snapshotPort)
		http.ListenAndServe(":"+*snapshotPort, mux)
	}()

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerServiceServer(grpcServer, srv)

	log.Printf("worker %s: gRPC listening on port %s", *workerID, *port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("worker %s: gRPC failed: %v", *workerID, err)
	}
}
