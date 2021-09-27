package replication

import (
	"eeylops/util/logging"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"
)

const kRaftDir = "raft"
const kRaftSnapshotDir = "snapshot_store"
const kRaftLogStoreDir = "log_store"

type RaftController struct {
	raft         *raft.Raft
	rootDir      string
	controllerID string
	localAddr    string
	logger       *logging.PrefixLogger
}

type RaftControllerState int

// Represents the Raft controller states
const (
	Leader RaftControllerState = iota
	Follower
	Candidate
	Shutdown
	Unknown
)

func NewRaftController(localID string, bindAddr string, rootDir string, fsm raft.FSM) *RaftController {
	controller := new(RaftController)
	controller.rootDir = rootDir
	controller.controllerID = localID
	controller.localAddr = bindAddr
	controller.logger = logging.NewPrefixLogger(fmt.Sprintf("Raft: %s", controller.controllerID))
	controller.raft = buildRaft(localID, bindAddr, rootDir, fsm, controller.logger)
	return controller
}

// RootDir returns the path to the controller's storage directory.
func (rc *RaftController) RootDir() string {
	return rc.rootDir
}

// Addr returns the address of the controller.
func (rc *RaftController) Addr() string {
	return rc.localAddr
}

// ID returns the controller ID.
func (rc *RaftController) ID() string {
	return rc.controllerID
}

// IsLeader returns true if this controller is the leader. false otherwise.
func (rc *RaftController) IsLeader() bool {
	return false
}

// State returns the current raft controller state.
func (rc *RaftController) State() RaftControllerState {
	state := rc.raft.State()
	switch state {
	case raft.Leader:
		return Leader
	case raft.Candidate:
		return Candidate
	case raft.Follower:
		return Follower
	case raft.Shutdown:
		return Shutdown
	default:
		return Unknown
	}
}

// LeaderAddr returns the address of the current leader. Returns an empty string if there is no leader.
func (rc *RaftController) LeaderAddr() (string, error) {
	return string(rc.raft.Leader()), nil
}

// LeaderID returns the controller ID of the Raft leader. Returns an empty string if there is no leader, or if
// there was an error.
func (rc *RaftController) LeaderID() (string, error) {
	addr, err := rc.LeaderAddr()
	if err != nil {
		return "", nil
	}
	configFuture := rc.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		rc.logger.Errorf("Failed to get raft configuration due to err: %v", err)
		return "", err
	}

	for _, srv := range configFuture.Configuration().Servers {
		if srv.Address == raft.ServerAddress(addr) {
			return string(srv.ID), nil
		}
	}
	return "", nil
}

// WaitForLeader waits till a leader is detected and returns the leader address. Returns an empty string and
// ErrRaftTimeout if a leader was not detected before 'timeout' expires.
func (rc *RaftController) WaitForLeader(timeout time.Duration) (string, error) {
	tck := time.NewTicker(time.Millisecond * 10)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			leader, err := rc.LeaderAddr()
			if err != nil {
				return "", nil
			}
			if leader != "" {
				return leader, nil
			}
			// No error and leader address is ""? Just continue till timeout expires.
			continue
		case <-tmr.C:
			return "", ErrRaftTimeout
		}
	}
}

func buildRaft(localID string, bindAddr string, rootDir string, fsm raft.FSM, logger *logging.PrefixLogger) *raft.Raft {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		logger.Fatalf("Unable to bind to address: %s due to err: %v", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 48, 10*time.Second, os.Stderr)
	if err != nil {
		logger.Fatalf("Unable to create TCP transport due to err: %v", err)
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(getRaftSnapshotDir(rootDir), 1, os.Stderr)
	if err != nil {
		logger.Fatalf("Unable to initialize snapshot store due to err: %v", err)
	}

	// Create the log store and stable store.
	// TODO: Use raft badger DB instead of bolt. There is a package already out there. We can
	// TODO: reuse that. Let's also see if we can merge that with our KV store impl if required.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	logStoreDir := getLogStoreDir(rootDir)
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(logStoreDir, "raft.db"))
	if err != nil {
		logger.Fatalf("Unable to create bolt db store for raft due to err: %v", err)
	}
	logStore = boltDB
	stableStore = boltDB

	// Start raft.
	ra, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshots, transport)
	if err != nil {
		logger.Fatalf("Unable to initialize Raft due to err: %v", err)
	}
	return ra
}

func getRaftDir(rootDir string) string {
	return path.Join(rootDir, kRaftDir)
}

func getLogStoreDir(rootDir string) string {
	return path.Join(getRaftDir(rootDir), kRaftLogStoreDir)
}

func getRaftSnapshotDir(rootDir string) string {
	return path.Join(getRaftDir(rootDir), kRaftSnapshotDir)
}
