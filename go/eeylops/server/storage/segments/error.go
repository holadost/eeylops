package segments

import "errors"

var (
	// ErrSegmentGeneric when we have an unexpected segment error.
	ErrSegmentGeneric = errors.New("ErrSegmentGeneric: generic segment error")

	// ErrSegmentBackend when we have an unexpected segment error.
	ErrSegmentBackend = errors.New("ErrSegmentBackend: segment backend error")

	// ErrSegmentClosed is returned if we try to access the segment when it is closed.
	ErrSegmentClosed = errors.New("ErrSegmentClosed: segment is closed")

	// ErrSegmentInvalid is returned if we try to access the segment when it is closed.
	ErrSegmentInvalid = errors.New("ErrSegmentClosed: segment is closed")

	ErrSegmentInvalidRLogIdx   = errors.New("ErrSegmentInvalidRLogIdx: invalid replicated log index")
	ErrSegmentInvalidTimestamp = errors.New("ErrSegmentInvalidTimestamp: invalid timestamp")
)
