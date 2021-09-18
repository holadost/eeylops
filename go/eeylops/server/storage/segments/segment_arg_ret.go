package segments

import storagebase "eeylops/server/storage/base"

type AppendEntriesArg struct {
	storagebase.AppendEntriesArg
}

type AppendEntriesRet struct {
	storagebase.AppendEntriesRet
}

type ScanEntriesArg struct {
	storagebase.ScanEntriesArg
	ScanSizeBytes int64
}

type ScanEntriesRet struct {
	storagebase.ScanEntriesRet
}
