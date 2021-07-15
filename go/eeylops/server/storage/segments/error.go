package segments

import "errors"

var (
	// ErrGenericSegment when we have an unexpected segment error.
	ErrGenericSegment = errors.New("ErrGenericSegment: generic segment error")

	// ErrSegmentBackend when we have an unexpected segment error.
	ErrSegmentBackend = errors.New("ErrSegmentBackend: segment backend error")

	// ErrSegmentClosed is returned if we try to access the segment when it is closed.
	ErrSegmentClosed = errors.New("ErrSegmentClosed: segment is closed")
)
