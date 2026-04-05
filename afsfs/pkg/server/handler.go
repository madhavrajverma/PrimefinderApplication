package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	pb "afsfs/generated/afs"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const chunkSize = 1024 * 1024 // 1MB per chunk

// FileHandle represents one open file on the server side
type FileHandle struct {
	path    string
	isWrite bool
}

type dedupEntry struct {
	response interface{}
}

// Handler implements the AFSServiceServer interface
type Handler struct {
	pb.UnimplementedAFSServiceServer

	inputDir  string
	outputDir string

	handleMu sync.Mutex
	handles  map[int64]*FileHandle

	versionMu sync.Mutex
	versions  map[string]int64

	nextHandle atomic.Int64

	dedupMu    sync.Mutex
	dedupTable map[string]dedupEntry

	serverID   string
	serverAddr string

	primaryMu        sync.RWMutex
	primary          bool
	knownPrimaryAddr string

	peers   []*peerClient
	peersMu sync.RWMutex

	lastHeartbeat   time.Time
	lastHeartbeatMu sync.Mutex

	senderRunning atomic.Bool
}

func NewHandler(
	inputDir string,
	outputDir string,
	serverID string,
	serverAddr string,
	peerInfos []PeerInfo,
	isPrimary bool,
) (*Handler, error) {
	h := &Handler{
		inputDir:      inputDir,
		outputDir:     outputDir,
		handles:       make(map[int64]*FileHandle),
		versions:      make(map[string]int64),
		dedupTable:    make(map[string]dedupEntry),
		serverID:      serverID,
		serverAddr:    serverAddr,
		primary:       isPrimary,
		lastHeartbeat: time.Now(),
	}
	if isPrimary {
		h.knownPrimaryAddr = serverAddr
	}

	for _, p := range peerInfos {
		peer, err := newPeerClient(p.ID, p.Addr)
		if err != nil {
			log.Printf("warning: could not connect to peer %s at %s: %v", p.ID, p.Addr, err)
			continue
		}
		h.peers = append(h.peers, peer)
	}

	if err := h.initVersion(inputDir); err != nil {
		return nil, fmt.Errorf("scanning input dir: %w", err)
	}
	if err := h.initVersion(outputDir); err != nil {
		return nil, fmt.Errorf("scanning output dir: %w", err)
	}

	if isPrimary {
		h.startHeartbeatSender()
	} else {
		h.startHeartbeatMonitor()
		go h.syncFromPrimaryWithRetry()
	}

	return h, nil
}

func dialPeer(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// syncFromPrimaryWithRetry calls SyncState (streaming) on the primary
// to download all output files it missed.
func (h *Handler) syncFromPrimaryWithRetry() {
	for i := 0; i < 30; i++ {
		if i > 0 {
			time.Sleep(2 * time.Second)
		}

		primaryAddr := h.resolvePrimaryAddr()
		if primaryAddr == "" {
			log.Printf("server %s: cannot find primary yet (attempt %d)", h.serverID, i+1)
			continue
		}

		if primaryAddr == h.serverAddr {
			log.Printf("server %s: I am the primary, no sync needed", h.serverID)
			return
		}

		var stub pb.AFSServiceClient
		var rawConn *grpc.ClientConn

		for _, peer := range h.copyPeers() {
			if peer.addr == primaryAddr {
				stub = peer.stub
				break
			}
		}
		if stub == nil {
			conn, err := dialPeer(primaryAddr)
			if err != nil {
				log.Printf("server %s: cannot dial primary %s: %v", h.serverID, primaryAddr, err)
				continue
			}
			rawConn = conn
			stub = pb.NewAFSServiceClient(conn)
		}

		// SyncState is now server-streaming — primary sends files chunk by chunk
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		stream, err := stub.SyncState(ctx, &pb.SyncStateRequest{ServerId: h.serverID})
		if err != nil {
			cancel()
			if rawConn != nil {
				rawConn.Close()
			}
			log.Printf("server %s: SyncState from %s failed: %v", h.serverID, primaryAddr, err)
			continue
		}

		// Receive files chunk by chunk
		// New file starts when chunk.Path != ""
		// Same file continues when chunk.Path == ""
		synced := 0
		var (
			curPath    string
			curVersion int64
			curTmp     *os.File
			curFull    string
			curTmpPath string
		)

		failed := false
		for {
			chunk, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				log.Printf("server %s: SyncState recv error: %v", h.serverID, recvErr)
				failed = true
				break
			}

			// New file starts when Path is set
			if chunk.Path != "" && chunk.Path != curPath {
				// Finalize previous file if any
				if curTmp != nil {
					curTmp.Close()
					os.Rename(curTmpPath, curFull)
					h.versionMu.Lock()
					h.versions[curFull] = curVersion
					h.versionMu.Unlock()
					log.Printf("server %s: synced %s (version %d)", h.serverID, curPath, curVersion)
					synced++
					curTmp = nil
				}

				curPath = chunk.Path
				curVersion = chunk.Version
				curFull = filepath.Join(h.outputDir, filepath.Base(curPath))
				curTmpPath = curFull + ".tmp"

				// Skip if already up to date
				h.versionMu.Lock()
				existingVer, exists := h.versions[curFull]
				h.versionMu.Unlock()
				if exists && existingVer >= curVersion {
					curPath = ""
					continue
				}

				var ferr error
				curTmp, ferr = os.Create(curTmpPath)
				if ferr != nil {
					log.Printf("server %s: cannot create tmp for %s: %v", h.serverID, curPath, ferr)
					curPath = ""
					continue
				}
			}

			// Write chunk data
			if curTmp != nil && len(chunk.Data) > 0 {
				if _, err := curTmp.Write(chunk.Data); err != nil {
					log.Printf("server %s: write chunk error: %v", h.serverID, err)
					curTmp.Close()
					os.Remove(curTmpPath)
					curTmp = nil
					curPath = ""
				}
			}
		}

		// Finalize last file
		if curTmp != nil {
			curTmp.Close()
			os.Rename(curTmpPath, curFull)
			h.versionMu.Lock()
			h.versions[curFull] = curVersion
			h.versionMu.Unlock()
			log.Printf("server %s: synced %s (version %d)", h.serverID, curPath, curVersion)
			synced++
		}

		cancel()
		if rawConn != nil {
			rawConn.Close()
		}

		if failed {
			continue
		}

		log.Printf("server %s synced %d files from primary %s", h.serverID, synced, primaryAddr)
		return
	}
	log.Printf("server %s: could not sync from primary — starting fresh", h.serverID)
}

func (h *Handler) resolvePrimaryAddr() string {
	h.primaryMu.RLock()
	known := h.knownPrimaryAddr
	h.primaryMu.RUnlock()
	if known != "" && known != h.serverAddr {
		return known
	}
	for _, peer := range h.copyPeers() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := peer.stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
		cancel()
		if err == nil && resp.PrimaryAddr != "" {
			return resp.PrimaryAddr
		}
	}
	return ""
}

func (h *Handler) initVersion(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(dir, 0755)
		}
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			fullPath := filepath.Join(dir, entry.Name())
			h.versions[fullPath] = 1
		}
	}
	return nil
}

// ── Open (unary) ────────────────────────────────────────────────────────────

func (h *Handler) Open(ctx context.Context, req *pb.OpenRequest) (*pb.OpenResponse, error) {
	key := dedupKey(req.ClientId, req.ReqSeq)
	h.dedupMu.Lock()
	if entry, exists := h.dedupTable[key]; exists {
		h.dedupMu.Unlock()
		return entry.response.(*pb.OpenResponse), nil
	}
	h.dedupMu.Unlock()

	fullPath, err := h.resolvePath(req.Path)
	if err != nil {
		return &pb.OpenResponse{Error: err.Error()}, nil
	}

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return &pb.OpenResponse{Error: fmt.Sprintf("file not found: %s", req.Path)}, nil
	}

	id := h.nextHandle.Add(1)
	h.handleMu.Lock()
	h.handles[id] = &FileHandle{path: fullPath, isWrite: req.Write}
	h.handleMu.Unlock()

	resp := &pb.OpenResponse{FileHandleId: id}
	h.dedupMu.Lock()
	h.dedupTable[key] = dedupEntry{response: resp}
	h.dedupMu.Unlock()

	return resp, nil
}

// ── FetchFile (server streaming) ────────────────────────────────────────────
// Sends file in 1MB chunks. Each chunk carries Data + Version.

func (h *Handler) FetchFile(req *pb.FetchFileRequest, stream pb.AFSService_FetchFileServer) error {
	fullPath, err := h.resolvePath(req.Path)
	if err != nil {
		return stream.Send(&pb.FetchFileResponse{Error: err.Error()})
	}

	f, err := os.Open(fullPath)
	if err != nil {
		return stream.Send(&pb.FetchFileResponse{Error: fmt.Sprintf("open: %v", err)})
	}
	defer f.Close()

	h.versionMu.Lock()
	version := h.versions[fullPath]
	h.versionMu.Unlock()

	buf := make([]byte, chunkSize)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.FetchFileResponse{
				Data:    buf[:n],
				Version: version,
			}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// ── TestAuth (unary) ────────────────────────────────────────────────────────

func (h *Handler) TestAuth(ctx context.Context, req *pb.TestAuthRequest) (*pb.TestAuthResponse, error) {
	fullPath, err := h.resolvePath(req.Path)
	if err != nil {
		return &pb.TestAuthResponse{IsValid: false}, nil
	}
	h.versionMu.Lock()
	currentVersion := h.versions[fullPath]
	h.versionMu.Unlock()
	return &pb.TestAuthResponse{
		IsValid: currentVersion == req.CachedVersion,
		Version: currentVersion,
	}, nil
}

// ── StoreFile (client streaming) ────────────────────────────────────────────
// Receives file in 1MB chunks. First chunk has Path + ClientId + ReqSeq.

func (h *Handler) StoreFile(stream pb.AFSService_StoreFileServer) error {
	if !h.isPrimary() {
		return stream.SendAndClose(&pb.StoreFileResponse{Error: "not primary: send writes to primary"})
	}

	var (
		path     string
		clientID string
		reqSeq   int64
		tmpFile  *os.File
		fullPath string
		tmpPath  string
		first    = true
	)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if tmpFile != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
			}
			return err
		}

		if first {
			first = false
			path = chunk.Path
			clientID = chunk.ClientId
			reqSeq = chunk.ReqSeq

			key := dedupKey(clientID, reqSeq)
			h.dedupMu.Lock()
			if entry, exists := h.dedupTable[key]; exists {
				h.dedupMu.Unlock()
				return stream.SendAndClose(entry.response.(*pb.StoreFileResponse))
			}
			h.dedupMu.Unlock()

			fullPath = filepath.Join(h.outputDir, filepath.Base(path))
			tmpPath = fullPath + ".tmp"
			tmpFile, err = os.Create(tmpPath)
			if err != nil {
				return stream.SendAndClose(&pb.StoreFileResponse{
					Error: fmt.Sprintf("creating tmp: %v", err),
				})
			}
		}

		if len(chunk.Data) > 0 {
			if _, err := tmpFile.Write(chunk.Data); err != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
				return stream.SendAndClose(&pb.StoreFileResponse{
					Error: fmt.Sprintf("writing chunk: %v", err),
				})
			}
		}
	}

	if tmpFile == nil {
		return stream.SendAndClose(&pb.StoreFileResponse{Error: "no data received"})
	}
	tmpFile.Close()

	if err := os.Rename(tmpPath, fullPath); err != nil {
		os.Remove(tmpPath)
		return stream.SendAndClose(&pb.StoreFileResponse{Error: fmt.Sprintf("rename: %v", err)})
	}

	h.versionMu.Lock()
	h.versions[fullPath]++
	newVersion := h.versions[fullPath]
	h.versionMu.Unlock()

	// Synchronous replication — wait for all backups before ACKing client
	h.replicateStreamToAll("store", path, fullPath, newVersion)

	resp := &pb.StoreFileResponse{Version: newVersion}
	key := dedupKey(clientID, reqSeq)
	h.dedupMu.Lock()
	h.dedupTable[key] = dedupEntry{response: resp}
	h.dedupMu.Unlock()

	return stream.SendAndClose(resp)
}

// ── CreateFile (unary) ──────────────────────────────────────────────────────

func (h *Handler) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	if !h.isPrimary() {
		return &pb.CreateFileResponse{Error: "not primary: send writes to primary"}, nil
	}

	key := dedupKey(req.ClientId, req.ReqSeq)
	h.dedupMu.Lock()
	if entry, exists := h.dedupTable[key]; exists {
		h.dedupMu.Unlock()
		return entry.response.(*pb.CreateFileResponse), nil
	}
	h.dedupMu.Unlock()

	fullPath := filepath.Join(h.outputDir, filepath.Base(req.Path))
	if _, err := os.Stat(fullPath); err == nil {
		return &pb.CreateFileResponse{Error: fmt.Sprintf("file already exists: %s", req.Path)}, nil
	}

	fd, err := os.Create(fullPath)
	if err != nil {
		return &pb.CreateFileResponse{Error: fmt.Sprintf("creating file: %v", err)}, nil
	}
	fd.Close()

	h.versionMu.Lock()
	h.versions[fullPath] = 1
	h.versionMu.Unlock()

	id := h.nextHandle.Add(1)
	h.handleMu.Lock()
	h.handles[id] = &FileHandle{path: fullPath, isWrite: true}
	h.handleMu.Unlock()

	// Synchronous replication — wait for all backups before ACKing client
	h.replicateStreamToAll("create", req.Path, fullPath, 1)

	resp := &pb.CreateFileResponse{FileHandleId: id}
	h.dedupMu.Lock()
	h.dedupTable[key] = dedupEntry{response: resp}
	h.dedupMu.Unlock()

	return resp, nil
}

// ── DeleteFile (unary) ──────────────────────────────────────────────────────

func (h *Handler) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	if !h.isPrimary() {
		return &pb.DeleteFileResponse{Error: "not primary: send writes to primary"}, nil
	}

	key := dedupKey(req.ClientId, req.ReqSeq)
	h.dedupMu.Lock()
	if entry, exists := h.dedupTable[key]; exists {
		h.dedupMu.Unlock()
		return entry.response.(*pb.DeleteFileResponse), nil
	}
	h.dedupMu.Unlock()

	fullPath := filepath.Join(h.outputDir, filepath.Base(req.Path))
	if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
		return &pb.DeleteFileResponse{Error: fmt.Sprintf("deleting file: %v", err)}, nil
	}

	h.versionMu.Lock()
	delete(h.versions, fullPath)
	h.versionMu.Unlock()

	// Synchronous replication — wait for all backups before ACKing client
	h.replicateStreamToAll("delete", req.Path, fullPath, 0)

	resp := &pb.DeleteFileResponse{}
	h.dedupMu.Lock()
	h.dedupTable[key] = dedupEntry{response: resp}
	h.dedupMu.Unlock()

	return resp, nil
}

// ── CloseFile (unary) ───────────────────────────────────────────────────────

func (h *Handler) CloseFile(ctx context.Context, req *pb.CloseFileRequest) (*pb.CloseFileResponse, error) {
	h.handleMu.Lock()
	_, exists := h.handles[req.FileHandleId]
	if exists {
		delete(h.handles, req.FileHandleId)
	}
	h.handleMu.Unlock()

	if !exists {
		return &pb.CloseFileResponse{Error: fmt.Sprintf("unknown handle: %d", req.FileHandleId)}, nil
	}
	return &pb.CloseFileResponse{}, nil
}

// ── Replicate (client streaming) ────────────────────────────────────────────
// Receives chunks from primary. First chunk: Operation + Path + Version.

func (h *Handler) Replicate(stream pb.AFSService_ReplicateServer) error {
	var (
		operation string
		path      string
		version   int64
		tmpFile   *os.File
		fullPath  string
		tmpPath   string
		first     = true
	)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if tmpFile != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
			}
			return err
		}

		if first {
			first = false
			operation = chunk.Operation
			path = chunk.Path
			version = chunk.Version

			switch operation {
			case "delete":
				fp := filepath.Join(h.outputDir, filepath.Base(path))
				os.Remove(fp)
				h.versionMu.Lock()
				delete(h.versions, fp)
				h.versionMu.Unlock()
				return stream.SendAndClose(&pb.ReplicateResponse{})
			case "create":
				fp := filepath.Join(h.outputDir, filepath.Base(path))
				os.WriteFile(fp, []byte{}, 0644)
				h.versionMu.Lock()
				h.versions[fp] = version
				h.versionMu.Unlock()
				return stream.SendAndClose(&pb.ReplicateResponse{})
			}

			fullPath = filepath.Join(h.outputDir, filepath.Base(path))
			tmpPath = fullPath + ".tmp"
			tmpFile, err = os.Create(tmpPath)
			if err != nil {
				return stream.SendAndClose(&pb.ReplicateResponse{
					Error: fmt.Sprintf("create tmp: %v", err),
				})
			}
		}

		if tmpFile != nil && len(chunk.Data) > 0 {
			if _, err := tmpFile.Write(chunk.Data); err != nil {
				tmpFile.Close()
				os.Remove(tmpPath)
				return stream.SendAndClose(&pb.ReplicateResponse{
					Error: fmt.Sprintf("write chunk: %v", err),
				})
			}
		}
	}

	if tmpFile != nil {
		tmpFile.Close()
		if err := os.Rename(tmpPath, fullPath); err != nil {
			os.Remove(tmpPath)
			return stream.SendAndClose(&pb.ReplicateResponse{Error: fmt.Sprintf("rename: %v", err)})
		}
		h.versionMu.Lock()
		h.versions[fullPath] = version
		h.versionMu.Unlock()
	}

	return stream.SendAndClose(&pb.ReplicateResponse{})
}

// ── SyncState (server streaming) ────────────────────────────────────────────
// Sends all output files chunk by chunk to recovering server.

func (h *Handler) SyncState(req *pb.SyncStateRequest, stream pb.AFSService_SyncStateServer) error {
	log.Printf("SyncState requested by %s", req.ServerId)

	entries, err := os.ReadDir(h.outputDir)
	if err != nil {
		return nil
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fullPath := filepath.Join(h.outputDir, entry.Name())

		h.versionMu.Lock()
		version := h.versions[fullPath]
		h.versionMu.Unlock()

		f, err := os.Open(fullPath)
		if err != nil {
			continue
		}

		buf := make([]byte, chunkSize)
		firstChunk := true
		for {
			n, readErr := f.Read(buf)
			if n > 0 {
				chunk := &pb.SyncFileEntry{
					Data:    buf[:n],
					Version: version,
				}
				if firstChunk {
					chunk.Path = entry.Name()
					firstChunk = false
				}
				if err := stream.Send(chunk); err != nil {
					f.Close()
					return err
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				f.Close()
				return readErr
			}
		}
		f.Close()
	}
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func (h *Handler) resolvePath(name string) (string, error) {
	base := filepath.Base(name)
	if base == "." || base == ".." {
		return "", fmt.Errorf("invalid path %s", name)
	}
	inputPath := filepath.Join(h.inputDir, base)
	if _, err := os.Stat(inputPath); err == nil {
		return inputPath, nil
	}
	outputPath := filepath.Join(h.outputDir, base)
	if _, err := os.Stat(outputPath); err == nil {
		return outputPath, nil
	}
	return inputPath, nil
}

func dedupKey(clientID string, reqSeq int64) string {
	return fmt.Sprintf("%s:%d", clientID, reqSeq)
}
