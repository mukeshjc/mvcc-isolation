package mvcc

// loosest isolation at the top, strictest isolation at the bottom.
type IsolationLevel uint8

const (
	// that's pretty simple! But also pretty useless if your workload has conflicts.
	// If you can arrange your workload in a way where you know no concurrent transactions will ever read or write conflicting keys though, this could be pretty efficient!
	// The rules will only get more complex (and thus potentially more of a bottleneck) from here on.
	// But for the most part, people don't use this isolation level. SQLite, Yugabyte, Cockroach, and Postgres don't even implement it.
	// It is also not the default for any major database that does implement it.
	ReadUncommittedIsolation IsolationLevel = iota
	ReadCommittedIsolation
	RepeatableReadIsolation
	SnapshotIsolation
	SerializableIsolation
)
