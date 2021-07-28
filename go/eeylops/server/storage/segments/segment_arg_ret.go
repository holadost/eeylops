package segments

import sbase "eeylops/server/storage/base"

type AppendEntriesArg struct {
	sbase.AppendEntriesArg
}

type AppendEntriesRet struct {
	sbase.AppendEntriesRet
}

type ScanEntriesArg struct {
	sbase.ScanEntriesArg
	ScanSizeBytes int64
}

type ScanEntriesRet struct {
	sbase.ScanEntriesRet
}
