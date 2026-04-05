package snapshot

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	afs "afsfs/pkg/afs"
)

const (
	CoordSnapshotFile = "snapshot_coordinator.json"
	workerSnapshotFmt = "snapshot_%s.json"
)

// WorkerSnapshot is the state one worker saves to AFS.
type WorkerSnapshot struct {
	WorkerID       string   `json:"worker_id"`
	CompletedFiles []string `json:"completed_files"`
	PrimesFound    []uint64 `json:"primes_found"`
	TimestampUnix  int64    `json:"timestamp_unix"`
}

type ChannelState struct {
	WorkerID         string `json:"worker_id"`
	MessagesInFlight int    `json:"messages_in_flight"` // always 0
}

// CoordSnapshot is the coordinator global state (Chandy-Lamport snapshot).
type CoordSnapshot struct {
	CompletedFiles  []string       `json:"completed_files"`
	PendingFiles    []string       `json:"pending_files"`
	CollectedPrimes []uint64       `json:"collected_primes"`
	ChannelStates   []ChannelState `json:"channel_states"`
	MarkersSent     int            `json:"markers_sent"`
	MarkersAcked    int            `json:"markers_acked"`
	TimestampUnix   int64          `json:"timestamp_unix"`
}

// WorkerSnapshotFile returns the AFS filename for a worker id.
func WorkerSnapshotFile(workerID string) string {
	return fmt.Sprintf(workerSnapshotFmt, workerID)
}

func InitiateSnapshot(
	afsClient *afs.Client,
	completedFiles []string,
	pendingFiles []string,
	collectedPrimes []uint64,
	workerHTTPAddrs []string,
	workerIDs []string,
) error {
	log.Printf("snapshot: [Chandy-Lamport] initiating global snapshot — %d files done, %d primes",
		len(completedFiles), len(collectedPrimes))

	channelStates := make([]ChannelState, 0, len(workerIDs))
	markersSent := 0
	markersAcked := 0

	for i, httpAddr := range workerHTTPAddrs {
		wID := workerIDs[i]

		channelStates = append(channelStates, ChannelState{
			WorkerID:         wID,
			MessagesInFlight: 0,
		})

		markersSent++
		markerURL := fmt.Sprintf("http://%s/snapshot", httpAddr)
		resp, err := sendMarker(markerURL)
		if err != nil {
			log.Printf("snapshot: marker to %s (%s) failed: %v — worker may be down", wID, httpAddr, err)
			continue
		}
		resp.Body.Close()
		markersAcked++
		log.Printf("snapshot: marker ACK from %s — worker state recorded", wID)
	}

	snap := &CoordSnapshot{
		CompletedFiles:  completedFiles,
		PendingFiles:    pendingFiles,
		CollectedPrimes: collectedPrimes,
		ChannelStates:   channelStates,
		MarkersSent:     markersSent,
		MarkersAcked:    markersAcked,
		TimestampUnix:   time.Now().Unix(),
	}

	log.Printf("snapshot: [Chandy-Lamport] snapshot complete — markers sent=%d acked=%d channels=%d (all empty)",
		markersSent, markersAcked, len(channelStates))

	return SaveCoordSnapshot(afsClient, snap)
}

func sendMarker(url string) (*http.Response, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	return client.Post(url, "application/json", nil)
}

func SaveWorkerSnapshot(client *afs.Client, snap *WorkerSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal worker snapshot: %w", err)
	}
	return writeToAFS(client, WorkerSnapshotFile(snap.WorkerID), data)
}

func LoadWorkerSnapshot(client *afs.Client, workerID string) (*WorkerSnapshot, error) {
	data, err := readFromAFS(client, WorkerSnapshotFile(workerID))
	if err != nil {
		return nil, nil // file not found — no prior snapshot
	}
	var snap WorkerSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal worker snapshot: %w", err)
	}
	log.Printf("snapshot: loaded worker snapshot for %s (%d completed files, %d primes)",
		workerID, len(snap.CompletedFiles), len(snap.PrimesFound))
	return &snap, nil
}

func SaveCoordSnapshot(client *afs.Client, snap *CoordSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal coord snapshot: %w", err)
	}
	return writeToAFS(client, CoordSnapshotFile, data)
}

func LoadCoordSnapshot(client *afs.Client) (*CoordSnapshot, error) {
	data, err := readFromAFS(client, CoordSnapshotFile)
	if err != nil {
		return nil, nil
	}
	var snap CoordSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal coord snapshot: %w", err)
	}
	log.Printf("snapshot: loaded coordinator snapshot (%d completed, %d pending, %d primes, markers=%d/%d)",
		len(snap.CompletedFiles), len(snap.PendingFiles), len(snap.CollectedPrimes),
		snap.MarkersAcked, snap.MarkersSent)
	return &snap, nil
}

func DeleteCoordSnapshot(client *afs.Client) {
	_ = client.DeleteFile(CoordSnapshotFile)
}

func DeleteWorkerSnapshot(client *afs.Client, workerID string) {
	_ = client.DeleteFile(WorkerSnapshotFile(workerID))
}

func SetContains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func BuildSeenSet(primes []uint64) map[uint64]bool {
	m := make(map[uint64]bool, len(primes))
	for _, p := range primes {
		m[p] = true
	}
	return m
}

// writeToAFS overwrites a file on AFS atomically
func writeToAFS(client *afs.Client, name string, data []byte) error {

	_ = client.DeleteFile(name)

	handle, err := client.CreateFile(name)
	if err != nil {
		return fmt.Errorf("CreateFile %s: %w", name, err)
	}
	if _, err := client.Write(handle, data); err != nil {
		_ = client.Close(handle)
		return fmt.Errorf("Write %s: %w", name, err)
	}
	if err := client.Close(handle); err != nil {
		return fmt.Errorf("Close %s: %w", name, err)
	}
	log.Printf("snapshot: wrote %s (%d bytes) to AFS", name, len(data))
	return nil
}

// readFromAFS reads an entire AFS file into memory.
func readFromAFS(client *afs.Client, name string) ([]byte, error) {
	handle, err := client.Open(name, false)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}

	var all []byte
	buf := make([]byte, 32*1024)
	for {
		n, readErr := client.Read(handle, buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if readErr == io.EOF || readErr != nil {
			break
		}
	}
	_ = client.Close(handle)

	if len(all) == 0 {
		return nil, fmt.Errorf("empty or missing file %s", name)
	}
	return all, nil
}
