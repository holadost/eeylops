package replication

import "errors"

var (
	ErrRaftNotLeader = errors.New("not raft leader")
	ErrRaftTimeout   = errors.New("raft timeout")
)
