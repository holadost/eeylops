package segments

import "errors"

var (
	// ErrSegmentBackend when we have an unexpected segment error.
	ErrSegmentBackend = errors.New("ErrSegmentBackend: segment backend error")

	// ErrSegmentClosed is returned if we try to access the segment when it is closed.
	ErrSegmentClosed = errors.New("ErrSegmentClosed: segment is closed")

	// ErrSegmentInvalid is returned if we try to access the segment when it is closed.
	ErrSegmentInvalid = errors.New("ErrSegmentInvalid: segment is invalid")

	ErrSegmentInvalidRLogIdx   = errors.New("ErrSegmentInvalidRLogIdx: invalid replicated log index")
	ErrSegmentInvalidTimestamp = errors.New("ErrSegmentInvalidTimestamp: invalid timestamp")

	// ErrSegmentNoRecordsWithTimestamp is returned when no live records were found in the segment.
	ErrSegmentNoRecordsWithTimestamp = errors.New("no records found in segment with given timestamp")

	// errSegmentScanDoneEarly is returned when the scan finished early. Used internally.
	errSegmentScanDoneEarly = errors.New("scan finished. return early")
)
