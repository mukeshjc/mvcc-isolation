package mvcc

import (
	"github.com/tidwall/btree"
)

type Database struct {
	defaultIsolation  IsolationLevel
	store             map[string][]Value
	transactions      btree.Map[uint64, Transaction]
	nextTransactionId uint64
}

// the database itself will have a default isolation level that each transaction will inherit (for our own convenience in tests).
// the database will have a mapping of keys to an array of value versions. Later elements in the array will represent newer versions of a value.
// the database will also store the next free transaction id it will use to assign ids to new transactions.
//
// Note: To be thread-safe: store, transactions, and nextTransactionId should be guarded by a mutex.
//
//	But to keep the code small, this iteration will not use goroutines and thus does not need mutexes.
func NewDatabase() Database {
	return Database{
		defaultIsolation: ReadCommittedIsolation,
		store:            map[string][]Value{},
		// The `0` transaction id will be used to mean that
		// the id was not set. So all valid transaction ids
		// must start at 1.
		nextTransactionId: 1,
	}
}

func (d *Database) inprogress() btree.Set[uint64] {
	var ids btree.Set[uint64]
	iter := d.transactions.Iter()
	for ok := iter.First(); ok; ok = iter.Next() {
		if iter.Value().state == InProgressTransaction {
			ids.Insert(iter.Key())
		}
	}
	return ids
}

func (d *Database) newTransaction() *Transaction {
	t := Transaction{}
	t.isolation = d.defaultIsolation
	t.state = InProgressTransaction

	// Assign and increment transaction id.
	t.id = d.nextTransactionId
	d.nextTransactionId++

	// Store all inprogress transaction ids.
	t.inprogress = d.inprogress()

	// Add this transaction to history.
	d.transactions.Set(t.id, t)

	debug("starting transaction", t.id)

	return &t
}

// few more helpers for completing a transaction, for fetching a transaction by id, and for validating a transaction.
func (d *Database) completeTransaction(t *Transaction, state TransactionState) error {
	debug("completing transaction ", t.id)

	// Update transactions.
	t.state = state
	d.transactions.Set(t.id, *t)

	return nil
}

func (d *Database) transactionState(txId uint64) Transaction {
	t, ok := d.transactions.Get(txId)
	assert(ok, "valid transaction")
	return t
}

func (d *Database) assertValidTransaction(t *Transaction) {
	assert(t.id > 0, "valid id")
	assert(d.transactionState(t.id).state == InProgressTransaction, "in progress")
}
