package coordinator

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	afs "afsfs/pkg/afs"
	pb "primefinder/generated/prime"
	"primefinder/pkg/snapshot"
)

// Config struct holds all coordinator configuration.
type Config struct {
	AfsServers  []string
	WorkerAddrs []string
	CacheDir    string
	OutputFile  string
}

type workerResult struct {
	workerID string
	filePath string
	primes   []uint64
	count    int64
	elapsed  int64
	err      error
}

// main entry point for coordinator
func Run(cfg Config) error {
	log.Println("coordinator: connecting to AFS filesystem")
	afsClient, err := afs.NewClient(cfg.AfsServers, cfg.CacheDir)
	if err != nil {
		return fmt.Errorf("AFS connect: %w", err)
	}
	defer afsClient.CloseConn()

	// find all files on afs server input directory to process
	allInputFiles, err := discoverInputFiles(afsClient)

	if err != nil {
		return fmt.Errorf("discover files: %w", err)
	}
	if len(allInputFiles) == 0 {
		return fmt.Errorf("no input files found on AFS server")
	}

	log.Printf("coordinator: found %d input files: %v", len(allInputFiles), allInputFiles)

	// imp to recover if there exist any previous snapshot Check for existing coordinator snapshot (recovery path)
	var collectedPrimes []uint64
	var completedFiles []string

	existingSnap, snapErr := snapshot.LoadCoordSnapshot(afsClient)

	if snapErr != nil {
		log.Printf("coordinator: warning reading snapshot: %v — starting fresh", snapErr)
	}
	if existingSnap != nil {
		log.Printf("coordinator: RESUMING from snapshot — %d files done, %d primes already collected", len(existingSnap.CompletedFiles), len(existingSnap.CollectedPrimes))
		completedFiles = existingSnap.CompletedFiles
		collectedPrimes = existingSnap.CollectedPrimes
	}

	pendingFiles := filterPending(allInputFiles, completedFiles)
	log.Printf("coordinator: %d files pending (skipping %d already done)",
		len(pendingFiles), len(completedFiles))

	if len(pendingFiles) == 0 {
		log.Println("coordinator: all files already processed — writing final output")
		return writeFinalOutput(afsClient, cfg.OutputFile, collectedPrimes)
	}

	workers, err := connectToWorkers(cfg.WorkerAddrs)
	if err != nil {
		return fmt.Errorf("connect workers: %w", err)
	}
	log.Printf("coordinator: %d workers ready", len(workers))

	assignments := assignFiles(pendingFiles, workers)
	log.Println("coordinator: file assignments:")
	for wAddr, files := range assignments {
		log.Printf("  %s -> %v", wAddr, files)
	}

	seen := snapshot.BuildSeenSet(collectedPrimes)
	var mu sync.Mutex

	var wg sync.WaitGroup
	resultCh := make(chan workerResult, len(pendingFiles)+1)

	for _, w := range workers {
		files, ok := assignments[w.addr]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(worker *workerConn, filePaths []string) {
			defer wg.Done()
			for _, fp := range filePaths {
				res := processOneFile(worker, fp, cfg, workers)
				resultCh <- res
			}
		}(w, files)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	for res := range resultCh {
		if res.err != nil {
			log.Printf("coordinator: ERROR — worker %s failed on %s: %v",
				res.workerID, res.filePath, res.err)
			continue
		}

		log.Printf("coordinator: worker %s finished %s — %d primes from %d numbers in %dms",
			res.workerID, res.filePath, len(res.primes), res.count, res.elapsed)

		mu.Lock()
		for _, p := range res.primes {
			if !seen[p] {
				seen[p] = true
				collectedPrimes = append(collectedPrimes, p)
			}
		}
		completedFiles = append(completedFiles, res.filePath)
		snapCompleted := append([]string{}, completedFiles...)
		snapPending := filterPending(allInputFiles, completedFiles)
		snapPrimes := append([]uint64{}, collectedPrimes...)
		mu.Unlock()

		// Chandy Lampor Algo  initiate global snapshot with marker messages
		workerHTTPAddrs, workerIDs := buildWorkerHTTPAddrs(workers)
		if err := snapshot.InitiateSnapshot(
			afsClient,
			snapCompleted,
			snapPending,
			snapPrimes,
			workerHTTPAddrs,
			workerIDs,
		); err != nil {
			log.Printf("coordinator: warning  could not save snapshot: %v", err)
		}
	}

	log.Printf("coordinator: %d unique primes collected", len(collectedPrimes))

	if err := writeFinalOutput(afsClient, cfg.OutputFile, collectedPrimes); err != nil {
		return err
	}

	snapshot.DeleteCoordSnapshot(afsClient)
	for _, w := range workers {
		snapshot.DeleteWorkerSnapshot(afsClient, w.id)
	}

	return nil
}

func discoverInputFiles(client *afs.Client) ([]string, error) {
	var files []string
	for i := 1; i <= 9999; i++ {
		name := fmt.Sprintf("input_dataset_%03d.txt", i)
		handle, err := client.Open(name, false)
		if err != nil {
			break
		}
		client.Close(handle)
		files = append(files, name)
	}
	return files, nil
}

func filterPending(all, completed []string) []string {
	done := make(map[string]bool, len(completed))
	for _, f := range completed {
		done[f] = true
	}
	var pending []string
	for _, f := range all {
		if !done[f] {
			pending = append(pending, f)
		}
	}
	return pending
}

type workerConn struct {
	id   string
	addr string
	stub pb.WorkerServiceClient
	conn *grpc.ClientConn
}

func connectToWorkers(addrs []string) ([]*workerConn, error) {
	var workers []*workerConn
	for i, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("coordinator: cannot dial worker %s: %v", addr, err)
			continue
		}

		stub := pb.NewWorkerServiceClient(conn)
		id := fmt.Sprintf("w%d", i+1)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := stub.Health(ctx, &pb.HealthRequest{WorkerId: id})
		cancel()
		if err != nil || !resp.Alive {
			log.Printf("coordinator: worker %s at %s is NOT alive — skipping", id, addr)
			conn.Close()
			continue
		}
		log.Printf("coordinator: worker %s at %s is alive", id, addr)
		workers = append(workers, &workerConn{id: id, addr: addr, stub: stub, conn: conn})
	}

	if len(workers) == 0 {
		return nil, fmt.Errorf("no workers available")
	}
	return workers, nil
}

func assignFiles(files []string, workers []*workerConn) map[string][]string {
	assignments := make(map[string][]string)
	for i, file := range files {
		w := workers[i%len(workers)]
		assignments[w.addr] = append(assignments[w.addr], file)
	}
	return assignments
}

func processOneFile(w *workerConn, filePath string, cfg Config, allWorkers []*workerConn) workerResult {
	res := tryWorker(w, filePath, cfg)
	if res.err == nil {
		return res
	}
	log.Printf("coordinator: worker %s failed on %s (%v) — trying other workers", w.id, filePath, res.err)
	for _, other := range allWorkers {
		if other.id == w.id {
			continue
		}
		log.Printf("coordinator: reassigning %s to worker %s", filePath, other.id)
		res = tryWorker(other, filePath, cfg)
		if res.err == nil {
			log.Printf("coordinator: worker %s recovered %s", other.id, filePath)
			return res
		}
	}
	return workerResult{
		workerID: w.id,
		filePath: filePath,
		err:      fmt.Errorf("all workers failed for %s", filePath),
	}
}

func tryWorker(w *workerConn, filePath string, cfg Config) workerResult {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	defer cancel()

	stream, err := w.stub.ProcessFile(ctx, &pb.ProcessFileRequest{
		FilePath:   filePath,
		AfsServers: strings.Join(cfg.AfsServers, ","),
		CacheDir:   fmt.Sprintf("/tmp/afs-worker-%s", w.id),
		WorkerId:   w.id,
	})
	if err != nil {
		return workerResult{workerID: w.id, filePath: filePath, err: err}
	}

	// Receive all streamed chunks
	var allPrimes []uint64
	var count, elapsed int64

	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			return workerResult{workerID: w.id, filePath: filePath, err: recvErr}
		}
		if chunk.Error != "" {
			return workerResult{workerID: w.id, filePath: filePath, err: fmt.Errorf("%s", chunk.Error)}
		}
		allPrimes = append(allPrimes, chunk.Primes...)
		if chunk.Count > 0 {
			count = chunk.Count
		}
		if chunk.ElapsedMs > 0 {
			elapsed = chunk.ElapsedMs
		}
	}

	return workerResult{
		workerID: w.id,
		filePath: filePath,
		primes:   allPrimes,
		count:    count,
		elapsed:  elapsed,
	}
}

func writeFinalOutput(client *afs.Client, outputFile string, primes []uint64) error {
	unique := dedupAndSort(primes)
	log.Printf("coordinator: writing %d unique primes to %s", len(unique), outputFile)

	client.DeleteFile(outputFile)
	handle, err := client.CreateFile(outputFile)
	if err != nil {
		return fmt.Errorf("CreateFile %s: %w", outputFile, err)
	}

	var sb strings.Builder
	for _, p := range unique {
		sb.WriteString(fmt.Sprintf("%d\n", p))
	}

	if _, err := client.Write(handle, []byte(sb.String())); err != nil {
		client.Close(handle)
		return fmt.Errorf("Write %s: %w", outputFile, err)
	}
	if err := client.Close(handle); err != nil {
		return fmt.Errorf("Close %s: %w", outputFile, err)
	}
	log.Printf("coordinator: wrote %d primes to %s on AFS", len(unique), outputFile)
	return nil
}

func dedupAndSort(primes []uint64) []uint64 {
	seen := make(map[uint64]bool, len(primes))
	for _, p := range primes {
		seen[p] = true
	}
	unique := make([]uint64, 0, len(seen))
	for p := range seen {
		unique = append(unique, p)
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	return unique
}

func buildWorkerHTTPAddrs(workers []*workerConn) (httpAddrs []string, ids []string) {
	for _, w := range workers {
		parts := strings.SplitN(w.addr, ":", 2)
		host := parts[0]
		httpPort := "6100"
		if len(parts) == 2 {
			// Try to derive gRPC 6001 -> HTTP 6100, 6002 -> 6101.
			grpcPort := parts[1]
			if len(grpcPort) == 4 && grpcPort[0] == '6' {
				// Docker mode: all workers use internal port 6100
				httpPort = "6100"
			}
		}
		httpAddrs = append(httpAddrs, fmt.Sprintf("%s:%s", host, httpPort))
		ids = append(ids, w.id)
	}
	return
}
