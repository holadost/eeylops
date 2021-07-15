package storage

import "eeylops/server/base"

type ScanEntriesArg struct {
	StartOffset    base.Offset // Start offset
	NumMessages    uint64      // Number of messages to scan.
	StartTimestamp int64       // Start timestamp.
	EndTimestamp   int64       // End timestamp.
}

type ScanEntriesRet struct {
	Values     [][]byte    // The values.
	NextOffset base.Offset // Next offset to be scanned.
	Error      error       // Scan error if any.
}
