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
