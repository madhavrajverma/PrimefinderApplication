package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "afsfs/generated/afs"
	"afsfs/pkg/server"
)

func main() {
	id := flag.String("id", "s1", "server id")
	host := flag.String("host", "localhost", "hostname to advertise to clients")
	port := flag.String("port", "50051", "port to listen on")
	inputDir := flag.String("inputDir", "/tmp/afs-input", "input files directory")
	outputDir := flag.String("outputDir", "/tmp/afs-output", "output files directory")
	peers := flag.String("peers", "", "peer servers as id=addr,id=addr")
	primary := flag.Bool("primary", false, "start as primary")
	flag.Parse()

	var peerInfos []server.PeerInfo
	if *peers != "" {
		for _, p := range strings.Split(*peers, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peerInfos = append(peerInfos, server.PeerInfo{
					ID:   parts[0],
					Addr: parts[1],
				})
			}
		}
	}

	addr := fmt.Sprintf("%s:%s", *host, *port)
	log.Printf("Starting server id = %s addr = %s", *id, addr)

	startAsPrimary := *primary
	if startAsPrimary {
		existing := findExistingPrimary(peerInfos)
		if existing != "" && existing != addr {
			log.Printf("server %s: another primary exists at %s — starting as BACKUP", *id, existing)
			startAsPrimary = false
		}
	}

	handler, err := server.NewHandler(
		*inputDir, *outputDir, *id, addr, peerInfos, startAsPrimary,
	)
	if err != nil {
		log.Fatalf("failed to init handler: %v", err)
	}

	log.Printf("handler ready inputDir=%s outputDir=%s primary=%v", *inputDir, *outputDir, startAsPrimary)

	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v", *port, err)
	}
	log.Printf("listening on port %s", *port)

	grpcServer := grpc.NewServer()
	pb.RegisterAFSServiceServer(grpcServer, handler)

	log.Printf("server %s is running ...", *id)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func findExistingPrimary(peers []server.PeerInfo) string {
	for _, p := range peers {
		conn, err := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			continue
		}
		stub := pb.NewAFSServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := stub.GetPrimary(ctx, &pb.GetPrimaryRequest{})
		cancel()
		conn.Close()
		if err == nil && resp.PrimaryAddr != "" {
			return resp.PrimaryAddr
		}
	}
	return ""
}
