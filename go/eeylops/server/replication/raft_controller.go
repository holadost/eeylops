package replication

import (
	"github.com/golang/glog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
)

const kRaftDir = "raft"
const kRaftSnapshotDir = "snapshots"
const kRaftLogStoreDir = "log_store"

type RaftController struct {
	raft *raft.Raft
}

func NewController(localID string, bindAddr string, rootDir string, fsm raft.FSM) *RaftController {
	controller := new(RaftController)
	controller.raft = buildRaft(localID, bindAddr, rootDir, fsm)
	return controller
}

func buildRaft(localID string, bindAddr string, rootDir string, fsm raft.FSM) *raft.Raft {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		glog.Fatalf("Unable to bind to address: %s due to err: %v", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 48, 10*time.Second, os.Stderr)
	if err != nil {
		glog.Fatalf("Unable to create TCP transport due to err: %v", err)
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	raftDir := generateRaftDir(rootDir)
	snapshots, err := raft.NewFileSnapshotStore(path.Join(raftDir, kRaftSnapshotDir), 1, os.Stderr)
	if err != nil {
		glog.Fatalf("Unable to initialize snapshot store due to err: %v", err)
	}

	// Create the log store and stable store.
	// TODO: Use raft badger DB instead of bolt. There is a package already out there. We can
	// TODO: reuse that. Let's also see if we can merge that with our KV store impl if required.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	logStoreDir := path.Join(raftDir, kRaftLogStoreDir)
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(logStoreDir, "raft.db"))
	if err != nil {
		glog.Fatalf("Unable to create bolt db store for raft due to err: %v", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Start raft.
	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		glog.Fatalf("Unable to initialize Raft due to err: %v", err)
	}
	return ra
}

func generateRaftDir(rootDir string) string {
	return path.Join(rootDir, kRaftDir)
}
