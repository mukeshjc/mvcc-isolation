package mvcc

import (
	"fmt"

	"github.com/mukeshjc/mvcc-isolation/v2/utils"
)

// final bit of scaffolding we'll set up is an abstraction for database connections. A connection will have at most associated one transaction.
// users must ask the database for a new connection. Then within the connection they can manage a transaction.
type Connection struct {
	tx *Transaction
	db *Database
}

func (c *Connection) ExecCommand(command string, args []string) (string, error) {
	utils.Debug(command, args)

	// neat thing about MVCC is that beginning, committing, and aborting a transaction is metadata work.
	// it will not involve modifying any values we get, set, or delete.

	// begin a transaction, we ask the database for a new transaction and assign it to the current connection.
	if command == "begin" {
		utils.AssertEq(c.tx, nil, "no running transactions")
		c.tx = c.db.newTransaction()
		c.db.assertValidTransaction(c.tx)
		return fmt.Sprintf("%d", c.tx.id), nil
	}

	// abort/commit a transaction, we call the completeTransaction method (which makes sure the database transaction history gets updated)
	// with the AbortedTransaction/CommittedTransaction state.
	if command == "rollback" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, AbortedTransaction)
		c.tx = nil
		return "", err
	}

	if command == "commit" {
		c.db.assertValidTransaction(c.tx)
		err := c.db.completeTransaction(c.tx, CommittedTransaction)
		c.tx = nil
		return "", err
	}

	// "get" support, we'll iterate the list of value versions backwards for the key. And we'll call a special "isvisible" method to determine if this transaction can see this value.
	// The first value that passes the isvisible test is the correct value for the transaction.
	if command == "get" {
		c.db.assertValidTransaction(c.tx)

		key := args[0]

		// useful for stricter isolation levels
		c.tx.readset.Insert(key)

		for i := len(c.db.store[key]) - 1; i > -1; i-- {
			value := c.db.store[key][i]
			utils.Debug(value, c.tx, c.db.isVisible(c.tx, value))
			if c.db.isVisible(c.tx, value) {
				return value.value, nil
			}
		}

		return "", fmt.Errorf("cannot get key that doesn't exist")
	}

	// set and delete are similar to get. But this time when we walk the list of value versions, we will set the txEndId for the value to the current transaction id if the value version is visible to this transaction.
	if command == "set" || command == "delete" {
		c.db.assertValidTransaction(c.tx)

		key := args[0]

		// mark all visible versions as now invalid
		found := false
		for i := len(c.db.store[key]) - 1; i > -1; i-- {
			value := &c.db.store[key][i]
			utils.Debug(value, c.tx, c.db.isVisible(c.tx, *value))
			if c.db.isVisible(c.tx, *value) {
				value.txEndId = c.tx.id
				found = true
			}
		}

		if command == "delete" && !found {
			return "", fmt.Errorf("cannot delete key that doesn't exist")
		}

		// useful for stricter isolation levels
		c.tx.writeset.Insert(key)

		// for set, we'll append to the value version list with the new version of the value that starts at this current transaction.
		if command == "set" {
			value := args[1]
			c.db.store[key] = append(c.db.store[key], Value{
				txStartId: c.tx.id,
				txEndId:   0,
				value:     value,
			})

			return value, nil
		}

		// delete ok.
		return "", nil
	}

	return "", fmt.Errorf("%v command unimplemented", command)
}

func (c *Connection) MustExecCommand(cmd string, args []string) string {
	res, err := c.ExecCommand(cmd, args)
	utils.AssertEq(err, nil, "unexpected error")
	return res
}
