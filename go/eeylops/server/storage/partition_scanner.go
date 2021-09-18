package storage

import (
	"context"
	"eeylops/server/base"
	storagebase "eeylops/server/storage/base"
	"github.com/golang/glog"
)

type PartitionScanner struct {
	p                 *Partition
	startOffset       base.Offset
	lastNextOffset    base.Offset
	lastValuesScanned []*storagebase.ScanValue
	lastErrorScanned  error
}

func NewPartitionScanner(p *Partition, startOffset base.Offset) *PartitionScanner {
	var ps PartitionScanner
	ps.p = p
	ps.startOffset = startOffset
	return &ps
}

func (ps *PartitionScanner) Rewind() {
	ps.lastNextOffset = ps.startOffset
	ps.getVal()
}

func (ps *PartitionScanner) Valid() bool {
	if ps.lastErrorScanned != nil {
		return false
	}
	if ps.lastValuesScanned != nil {
		// We still have a value that has not been read. The iterator is still valid.
		return true
	}
	if ps.lastNextOffset == -1 {
		return false
	}
	return (ps.lastNextOffset != -1) && (ps.lastErrorScanned == nil)
}

func (ps *PartitionScanner) Next() {
	if ps.lastNextOffset == -1 || ps.lastErrorScanned != nil {
		return
	}
	ps.getVal()
}

func (ps *PartitionScanner) Get() (*storagebase.ScanValue, error) {
	if ps.lastErrorScanned != nil {
		return nil, ps.lastErrorScanned
	}
	if len(ps.lastValuesScanned) != 1 {
		glog.Fatalf("Expected one value. Got %d", len(ps.lastValuesScanned))
	}
	val := ps.lastValuesScanned[0]
	ps.lastValuesScanned = nil
	return val, nil
}

func (ps *PartitionScanner) Seek(offset base.Offset) {
	if ps.lastErrorScanned != nil {
		return
	}
	if offset >= ps.startOffset {
		ps.lastNextOffset = offset
	} else {
		glog.Fatalf("Invalid offset: %d lesser than start offset of scanner: %d", offset, ps.startOffset)
	}
	ps.lastValuesScanned = nil
	ps.getVal()
}

func (ps *PartitionScanner) getVal() {
	if ps.lastNextOffset == -1 {
		glog.Fatalf("Unexpected lastNextOffset: %d", ps.lastNextOffset)
	}
	arg := storagebase.ScanEntriesArg{
		StartOffset:    ps.lastNextOffset,
		NumMessages:    1,
		StartTimestamp: -1,
		EndTimestamp:   -1,
	}
	ret := ps.p.Scan(context.Background(), &arg)
	ps.lastValuesScanned = ret.Values
	ps.lastNextOffset = ret.NextOffset
	ps.lastErrorScanned = ret.Error
}
