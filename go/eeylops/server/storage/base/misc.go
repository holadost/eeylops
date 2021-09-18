package base

import (
	"eeylops/util"
	"github.com/golang/glog"
)

const KLastRLogIdxKey = "last_rlog_idx"

var (
	KLastRLogIdxKeyBytes = []byte(KLastRLogIdxKey)
)

func PrepareRLogIDXKeyVal(rlogIdx int64) ([]byte, []byte) {
	if rlogIdx < 0 {
		glog.Fatalf("Unexpected last log index: %d", rlogIdx)
	}
	return KLastRLogIdxKeyBytes, util.UintToBytes(uint64(rlogIdx))
}

func GetLastRLogKeyBytes() []byte {
	return KLastRLogIdxKeyBytes
}

func GetRLogValFromBytes(data []byte) int64 {
	if len(data) != 8 {
		glog.Fatalf("Unable to convert data to replicated log index as data size: %d != 8", len(data))
	}
	return int64(util.BytesToUint(data))
}

func PrepareRLogValBytes(rlogIdx int64) []byte {
	return util.UintToBytes(uint64(rlogIdx))
}
