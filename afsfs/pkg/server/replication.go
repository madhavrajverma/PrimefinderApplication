package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "afsfs/generated/afs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// PeerInfo holds the ID and address of a peer server
type PeerInfo struct {
	ID   string
	Addr string
}

// peerClient holds a gRPC connection to one peer server
type peerClient struct {
	id        string
	addr      string
	stub      pb.AFSServiceClient
	failCount atomic.Int64
}

func newPeerClient(id, addr string) (*peerClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connecting to peer %s: %w", addr, err)
	}
	return &peerClient{
		id:   id,
		addr: addr,
		stub: pb.NewAFSServiceClient(conn),
	}, nil
}

// replicateStreamToAll sends an operation to all backups in parallel using streaming.
func (h *Handler) replicateStreamToAll(operation, path, fullPath string, version int64) {
	h.peersMu.RLock()
	peers := make([]*peerClient, len(h.peers))
	copy(peers, h.peers)
	h.peersMu.RUnlock()

	if len(peers) == 0 {
		return
	}

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p *peerClient) {
			defer wg.Done()
			if err := h.replicateStreamToPeer(p, operation, path, fullPath, version); err != nil {
				log.Printf("streaming replication to %s failed: %v", p.addr, err)
			}
		}(peer)
	}
	wg.Wait()
}

// replicateStreamToPeer streams one operation to one backup.
// Uses protoc-generated Replicate client-streaming RPC.
func (h *Handler) replicateStreamToPeer(peer *peerClient, operation, path, fullPath string, version int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// stub.Replicate() returns AFSService_ReplicateClient (client streaming)
	stream, err := peer.stub.Replicate(ctx)
	if err != nil {
		return fmt.Errorf("Replicate stream: %w", err)
	}

	// delete and create — single chunk, no file data
	if operation == "delete" || operation == "create" {
		if err := stream.Send(&pb.ReplicateRequest{
			Operation: operation,
			Path:      path,
			Version:   version,
		}); err != nil {
			return err
		}
		resp, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("replicate error: %s", resp.Error)
		}
		return nil
	}

	// store — stream file in 1MB chunks
	f, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("open for replication: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 1024*1024)
	first := true
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := &pb.ReplicateRequest{Data: buf[:n]}
			if first {
				first = false
				chunk.Operation = operation
				chunk.Path = path
				chunk.Version = version
			}
			if err := stream.Send(chunk); err != nil {
				return fmt.Errorf("send chunk: %w", err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("CloseAndRecv: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("replicate error: %s", resp.Error)
	}
	return nil
}
