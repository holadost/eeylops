package storage

type AppendEntriesArg struct {
	Entries   [][]byte // The entries to be appended
	Timestamp int64    // The timestamp associated with the entries.
	RLogIdx   int64    // Replicated log index.
}

type AppendEntriesRet struct {
	Error error // Append error if any.
}
