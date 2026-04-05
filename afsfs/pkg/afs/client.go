package afs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "afsfs/generated/afs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the AFS client library.
type Client struct {
	grpcConn    *grpc.ClientConn
	stub        pb.AFSServiceClient
	cache       *cacheManager
	clientId    string
	reqSeq      atomic.Int64
	serverAddrs []string
	primaryAddr string
	primaryMu   sync.Mutex
}

func NewClient(serverAddrs []string, cacheDir string) (*Client, error) {
	if len(serverAddrs) == 0 {
		return nil, fmt.Errorf("need at least one server address")
	}

	cache, err := newCacheManager(cacheDir)
	if err != nil {
		return nil, fmt.Errorf("creating cache manager: %w", err)
	}

	clientID := fmt.Sprintf("client-%d-%d", os.Getpid(), time.Now().UnixNano())
	c := &Client{
		serverAddrs: serverAddrs,
		cache:       cache,
		clientId:    clientID,
	}

	if err := c.connectToPrimary(); err != nil {
		return nil, fmt.Errorf("finding primary: %w", err)
	}

	return c, nil
}

func (c *Client) connectToPrimary() error {
	for _, addr := range c.serverAddrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		stub := pb.NewAFSServiceClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
		cancel()
		conn.Close()

		if err != nil || resp.PrimaryAddr == "" {
			continue
		}

		primaryAddr := resp.PrimaryAddr
		primaryConn, err := grpc.NewClient(primaryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		primaryStub := pb.NewAFSServiceClient(primaryConn)

		pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, pingErr := primaryStub.Heartbeat(pingCtx, &pb.HeartbeatRequest{ServerId: "client"})
		pingCancel()
		if pingErr != nil {
			primaryConn.Close()
			log.Printf("client: primary %s reported by %s is unreachable: %v", primaryAddr, addr, pingErr)
			continue
		}

		c.grpcConn = primaryConn
		c.stub = primaryStub
		c.primaryAddr = primaryAddr
		log.Printf("connected to primary at %s", primaryAddr)
		return nil
	}
	return fmt.Errorf("could not find primary among %v", c.serverAddrs)
}

func (c *Client) reconnectToPrimary() error {
	c.primaryMu.Lock()
	defer c.primaryMu.Unlock()
	log.Printf("client: primary failed or not primary — reconnecting...")
	if c.grpcConn != nil {
		c.grpcConn.Close()
		c.grpcConn = nil
	}
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			wait := time.Duration(500+attempt*500) * time.Millisecond
			log.Printf("client: waiting %v before retry %d to find new primary", wait, attempt+1)
			time.Sleep(wait)
		}
		if err := c.connectToPrimary(); err == nil {
			return nil
		}
	}
	return fmt.Errorf("could not find primary after election among %v", c.serverAddrs)
}

func (c *Client) CloseConn() {
	c.primaryMu.Lock()
	defer c.primaryMu.Unlock()
	if c.grpcConn != nil {
		c.grpcConn.Close()
	}
}

// Open opens a remote file and caches it locally.
func (c *Client) Open(path string, write bool) (int64, error) {
	seq := c.reqSeq.Add(1)

	openResp, err := retryWithFailover(c, func(ctx context.Context) (*pb.OpenResponse, error) {
		return c.stub.Open(ctx, &pb.OpenRequest{
			Path:     path,
			Write:    write,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.OpenResponse) string { return r.Error })

	if err != nil {
		return 0, fmt.Errorf("Open RPC failed: %w", err)
	}
	if openResp.Error != "" {
		return 0, fmt.Errorf("Open RPC error: %s", openResp.Error)
	}

	serverHandleID := openResp.FileHandleId

	// Check local cache first
	cachedVersion, inCache := c.cache.isCached(path)
	if inCache {
		authResp, err := retryRPC(func(ctx context.Context) (*pb.TestAuthResponse, error) {
			return c.stub.TestAuth(ctx, &pb.TestAuthRequest{
				Path:          path,
				CachedVersion: cachedVersion,
			})
		})
		if err == nil && authResp.IsValid {
			localPath := c.cache.localPath(path)
			fd, err := openLocalFile(localPath, write)
			if err != nil {
				return 0, fmt.Errorf("opening local cache: %w", err)
			}
			clientHandle := c.cache.openFile(path, fd, serverHandleID)
			return clientHandle, nil
		}
	}

	// FetchFile using server-streaming — receives file in 1MB chunks.
	// Uses protoc-generated stub.FetchFile() → AFSService_FetchFileClient.
	// No message size limit — works for files of any size.
	localPath := c.cache.localPath(path)
	var fetchVersion int64

	fetchErr := retryWithFailoverRaw(c, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		// stub.FetchFile returns AFSService_FetchFileClient (server streaming)
		stream, err := c.stub.FetchFile(ctx, &pb.FetchFileRequest{Path: path})
		if err != nil {
			return fmt.Errorf("FetchFile stream: %w", err)
		}

		tmpPath := localPath + ".tmp"
		f, err := os.Create(tmpPath)
		if err != nil {
			return fmt.Errorf("create tmp: %w", err)
		}

		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				os.Remove(tmpPath)
				return fmt.Errorf("recv chunk: %w", err)
			}
			if chunk.Error != "" {
				f.Close()
				os.Remove(tmpPath)
				return fmt.Errorf("server error: %s", chunk.Error)
			}
			if len(chunk.Data) > 0 {
				if _, err := f.Write(chunk.Data); err != nil {
					f.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("write chunk: %w", err)
				}
			}
			if chunk.Version > 0 {
				fetchVersion = chunk.Version
			}
		}
		f.Close()
		return os.Rename(tmpPath, localPath)
	})
	if fetchErr != nil {
		return 0, fmt.Errorf("FetchFile RPC failed: %w", fetchErr)
	}

	c.cache.storeCacheEntry(path, fetchVersion)

	fd, err := openLocalFile(localPath, write)
	if err != nil {
		return 0, fmt.Errorf("opening local cache file: %w", err)
	}

	clientHandle := c.cache.openFile(path, fd, serverHandleID)
	return clientHandle, nil
}

func (c *Client) Read(handleID int64, buf []byte) (int, error) {
	entry, exists := c.cache.getOpenEntry(handleID)
	if !exists {
		return 0, fmt.Errorf("unknown handle: %d", handleID)
	}
	n, err := entry.localFd.Read(buf)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("reading local file: %w", err)
	}
	return n, err
}

func (c *Client) Write(handleID int64, data []byte) (int, error) {
	entry, exists := c.cache.getOpenEntry(handleID)
	if !exists {
		return 0, fmt.Errorf("unknown handle: %d", handleID)
	}
	n, err := entry.localFd.Write(data)
	if err != nil {
		return 0, fmt.Errorf("writing local file: %w", err)
	}
	c.cache.markDirty(handleID)
	return n, nil
}

// Close flushes dirty files to server using client-streaming StoreFile.
func (c *Client) Close(handleID int64) error {
	entry, exists := c.cache.closeFile(handleID)
	if !exists {
		return fmt.Errorf("unknown handle: %d", handleID)
	}

	entry.localFd.Close()

	if entry.dirty {
		localPath := c.cache.localPath(entry.path)
		seq := c.reqSeq.Add(1)

		// StoreFile using client-streaming — sends file in 1MB chunks.
		// Uses protoc-generated stub.StoreFile() → AFSService_StoreFileClient.
		// No message size limit — works for files of any size.
		var storeVersion int64
		storeErr := retryWithFailoverRaw(c, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			// stub.StoreFile returns AFSService_StoreFileClient (client streaming)
			stream, err := c.stub.StoreFile(ctx)
			if err != nil {
				return fmt.Errorf("StoreFile stream: %w", err)
			}

			f, err := os.Open(localPath)
			if err != nil {
				return fmt.Errorf("open local cache: %w", err)
			}
			defer f.Close()

			buf := make([]byte, 1024*1024)
			first := true
			for {
				n, readErr := f.Read(buf)
				if n > 0 {
					chunk := &pb.StoreFileRequest{Data: buf[:n]}
					if first {
						first = false
						chunk.Path = entry.path
						chunk.ClientId = c.clientId
						chunk.ReqSeq = seq
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
			if strings.Contains(resp.Error, "not primary") {
				return fmt.Errorf("not primary: %s", resp.Error)
			}
			if resp.Error != "" {
				return fmt.Errorf("StoreFile error: %s", resp.Error)
			}
			storeVersion = resp.Version
			return nil
		})
		if storeErr != nil {
			return fmt.Errorf("StoreFile RPC failed: %w", storeErr)
		}
		c.cache.updateVersion(entry.path, storeVersion)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	closeResp, err := c.stub.CloseFile(ctx, &pb.CloseFileRequest{FileHandleId: entry.serverHandleID})
	if err != nil {
		log.Printf("CloseFile RPC warning: %v", err)
		return nil
	}
	if closeResp.Error != "" {
		log.Printf("CloseFile RPC warning: %s", closeResp.Error)
	}
	return nil
}

// CreateFile creates a new output file on the primary.
func (c *Client) CreateFile(path string) (int64, error) {
	seq := c.reqSeq.Add(1)

	resp, err := retryWithFailover(c, func(ctx context.Context) (*pb.CreateFileResponse, error) {
		return c.stub.CreateFile(ctx, &pb.CreateFileRequest{
			Path:     path,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.CreateFileResponse) string { return r.Error })

	if err != nil {
		return 0, fmt.Errorf("CreateFile RPC failed: %w", err)
	}
	if resp.Error != "" {
		return 0, fmt.Errorf("CreateFile RPC error: %s", resp.Error)
	}

	localPath := c.cache.localPath(path)
	if err := os.WriteFile(localPath, []byte{}, 0644); err != nil {
		return 0, fmt.Errorf("creating local cache file: %w", err)
	}

	c.cache.storeCacheEntry(path, 1)

	fd, err := openLocalFile(localPath, true)
	if err != nil {
		return 0, fmt.Errorf("opening local cache file: %w", err)
	}

	clientHandle := c.cache.openFile(path, fd, resp.FileHandleId)
	return clientHandle, nil
}

// DeleteFile deletes a file from AFS.
func (c *Client) DeleteFile(path string) error {
	seq := c.reqSeq.Add(1)

	resp, err := retryWithFailover(c, func(ctx context.Context) (*pb.DeleteFileResponse, error) {
		return c.stub.DeleteFile(ctx, &pb.DeleteFileRequest{
			Path:     path,
			ClientId: c.clientId,
			ReqSeq:   seq,
		})
	}, func(r *pb.DeleteFileResponse) string { return r.Error })

	if err != nil {
		return fmt.Errorf("DeleteFile RPC failed: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("DeleteFile RPC error: %s", resp.Error)
	}

	localPath := c.cache.localPath(path)
	os.Remove(localPath)

	c.cache.mu.Lock()
	delete(c.cache.cached, path)
	c.cache.mu.Unlock()

	return nil
}

func openLocalFile(path string, write bool) (*os.File, error) {
	if write {
		return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	}
	return os.Open(path)
}

// retryWithFailover retries an RPC, reconnecting to new primary on failure.
func retryWithFailover[T any](
	c *Client,
	fn func(ctx context.Context) (T, error),
	getAppError func(T) string,
) (T, error) {
	const maxRetries = 8
	const timeout = 5 * time.Second

	var zero T
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond * time.Duration(attempt))
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		result, err := fn(ctx)
		cancel()

		if err != nil {
			lastErr = err
			log.Printf("client: RPC error (attempt %d): %v — trying reconnect", attempt+1, err)
			if reconnErr := c.reconnectToPrimary(); reconnErr != nil {
				log.Printf("client: reconnect failed: %v", reconnErr)
			}
			continue
		}

		if appErr := getAppError(result); strings.Contains(appErr, "not primary") {
			lastErr = fmt.Errorf("not primary: %s", appErr)
			log.Printf("client: received 'not primary' (attempt %d) — reconnecting", attempt+1)
			if reconnErr := c.reconnectToPrimary(); reconnErr != nil {
				log.Printf("client: reconnect failed: %v", reconnErr)
			}
			continue
		}

		return result, nil
	}

	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// retryRPC retries a read-only RPC without failover.
func retryRPC[T any](fn func(ctx context.Context) (T, error)) (T, error) {
	const maxRetries = 3
	const timeout = 5 * time.Second
	const baseDelay = 100 * time.Millisecond

	var zero T
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(baseDelay * time.Duration(1<<(attempt-1)))
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		result, err := fn(ctx)
		cancel()
		if err == nil {
			return result, nil
		}
		lastErr = err
	}
	return zero, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// retryWithFailoverRaw retries a streaming RPC that returns plain error.
func retryWithFailoverRaw(c *Client, fn func() error) error {
	const maxRetries = 8

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond * time.Duration(attempt))
			if err := c.reconnectToPrimary(); err != nil {
				log.Printf("client: reconnect failed: %v", err)
			}
		}
		if err := fn(); err != nil {
			lastErr = err
			log.Printf("client: streaming RPC error (attempt %d): %v", attempt+1, err)
			continue
		}
		return nil
	}
	return fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}
