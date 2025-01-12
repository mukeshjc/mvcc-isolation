package mvcc

// loosest isolation at the top, strictest isolation at the bottom.
type IsolationLevel uint8

const (
	ReadUncommittedIsolation IsolationLevel = iota
	ReadCommittedIsolation
	RepeatableReadIsolation
	SnapshotIsolation
	SerializableIsolation
)
