package mvcc

import (
	"github.com/tidwall/btree"
)

// transaction will be in an in-progress, rolledback, or committed state.
type TransactionState uint8

const (
	InProgressTransaction TransactionState = iota
	RolledBackTransaction
	CommittedTransaction
)

// transaction has an isolation level, an id (monotonic increasing integer), and a current state.
// And although we won't make use of this data yet, transactions at stricter isolation levels will need some extra info.
// Specifically, stricter isolation levels need to know about other transactions that were in-progress when this one started.
// And stricter isolation levels need to know about all keys read and written by a transaction.
type Transaction struct {
	isolation IsolationLevel
	id        uint64
	state     TransactionState

	// Used only by Repeatable Read and stricter.
	inprogress btree.Set[uint64]

	// Used only by Snapshot Isolation and stricter.
	writeset btree.Set[string]
	readset  btree.Set[string]
}
