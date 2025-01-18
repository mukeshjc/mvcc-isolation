package mvcc

import (
	"github.com/tidwall/btree"

	"github.com/mukeshjc/mvcc-isolation/v2/utils"
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
func NewDatabase(isolationLevel IsolationLevel) Database {
	return Database{
		defaultIsolation: isolationLevel,
		store:            map[string][]Value{},
		// the `0` transaction id will be used to mean that
		// the id was not set. So all valid transaction ids
		// must start at 1.
		nextTransactionId: 1,
	}
}

func (d *Database) NewConnection() *Connection {
	return &Connection{
		db: d,
		tx: nil,
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

	utils.Debug("starting transaction", t.id)

	return &t
}

// few more helpers for completing a transaction, for fetching a transaction by id, and for validating a transaction.
func (d *Database) completeTransaction(t *Transaction, state TransactionState) error {
	utils.Debug("completing transaction ", t.id)

	// update transactions.
	t.state = state
	d.transactions.Set(t.id, *t)

	return nil
}

func (d *Database) transactionState(txId uint64) Transaction {
	t, ok := d.transactions.Get(txId)
	utils.Assert(ok, "valid transaction")
	return t
}

func (d *Database) assertValidTransaction(t *Transaction) {
	utils.Assert(t.id > 0, "valid id")
	utils.Assert(d.transactionState(t.id).state == InProgressTransaction, "in progress")
}

func (d *Database) isVisible(t *Transaction, value Value) bool {
	// ReadUncommitted, has almost no restrictions. we can merely read the most recent (non-deleted) version of a value,
	// regardless of if the transaction that set it has committed or rolledback or not.
	// https://jepsen.io/consistency/models/read-uncommitted
	if t.isolation == ReadUncommittedIsolation {
		// we must merely make sure the value hasn't been deleted, that's all
		return value.txEndId == 0 // txEndId is not set, indicating the value hasn't been deleted by any tx
	}

	// we'll make sure that the value has a txStartId that is either this transaction or a transaction that has committed.
	// Moreover we will now begin checking against txEndId to make sure the value wasn't deleted by any relevant transaction.
	// this is useful and hence is the default isolation level for many databases including Postgres, Yugabyte, Oracle, and SQL Server
	// https://jepsen.io/consistency/models/read-committed
	if t.isolation == ReadCommittedIsolation {
		// If the value wasn't created by current transaction and the other transaction that created it isn't committed yet, then it's no good.
		if value.txStartId != t.id && d.transactionState(value.txStartId).state != CommittedTransaction {
			return false
		}

		// If the value was deleted ...
		if value.txEndId > 0 {
			// ... in the current transaction, then it's no good
			if value.txEndId == t.id {
				return false
			}

			// ... by other transaction that is committed, then it's no good.
			if d.transactionState(value.txEndId).state == CommittedTransaction {
				return false
			}
		}

		// now the value is useable. It's either created in current transaction or was created by other transaction that has committed
		return true

		// Even with this isolation level, you can easily get inconsistent data within a transaction at this isolation level.
		// If the transaction A has multiple statements it can see different results per statement, even if the transaction A did not modify data.
		// Another transaction B may have committed changes between two statements in this transaction A.
	}

	// Repeatable Read, Snapshot Isolation and Serializable further restricts Read Committed so only versions from transactions that completed before this one started are 	visible.
	// we will add additional checks for the Read Committed logic that make sure the value was not created and not deleted within a transaction that started before this transaction started.
	// As it happens, this is the same logic that will be necessary for Snapshot Isolation and Serializable Isolation.
	// The additional logic (that makes Snapshot Isolation and Serializable Isolation different) happens at commit time.

	utils.Assert(t.isolation == RepeatableReadIsolation || t.isolation == SnapshotIsolation || t.isolation == SerializableIsolation, "unsupported isolation level")

	////// now the specifics for a RepeatableReadIsolation level and above, rest of the checks for stricter isolation levels happens at Commit Time.

	// ignore values from transactions started after the current one
	if value.txStartId > t.id {
		return false
	}

	// ignore values created from transactions in-progress i.e. ongoing when this transaction began but may have committed when this transaction was in progress.
	// if we didn't check for this, then our current transaction may have performed some reads at the beginning, then an in-progress transaction committed and if we made
	// another read, we might see the values because now that would be a committed transaction as per ReadCommittedIsolation level. Thus it would be a dirty read and violate
	// RepeatableReadIsolation guarantee.
	if t.inprogress.Contains(value.txStartId) {
		return false
	}

	////// a copy of all checks we did for ReadUncommittedIsolation is below with slight **MODIFICATION** to the second statement in the bigger IF block

	// If the value wasn't created by current transaction and the other transaction that created it isn't committed yet, then it's no good.
	if value.txStartId != t.id && d.transactionState(value.txStartId).state != CommittedTransaction {
		return false
	}

	// If the value was deleted ...
	if value.txEndId > 0 {
		// ... in the current transaction, then it's no good
		if value.txEndId == t.id {
			return false
		}

		// ... by other transaction **that began before the current one** and it is committed, then it's no good.
		if value.txEndId < t.id && d.transactionState(value.txEndId).state == CommittedTransaction {
			return false
		}
	}

	return true
}
