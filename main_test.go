package main

import (
	"testing"

	"github.com/mukeshjc/mvcc-isolation/v2/mvcc"
	"github.com/mukeshjc/mvcc-isolation/v2/utils"
)

func TestReadUncommitted(t *testing.T) {
	database := mvcc.NewDatabase(mvcc.ReadUncommittedIsolation)
	c1 := database.NewConnection()
	c1.MustExecCommand("begin", nil)

	c2 := database.NewConnection()
	c2.MustExecCommand("begin", nil)

	c1.MustExecCommand("set", []string{"x", "hey"})

	// update is visible to self.
	res := c1.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c1 get x")

	// but since read uncommitted, also available to everyone else.
	res = c2.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c2 get x")

	// and if we delete, that should be respected.
	res = c1.MustExecCommand("delete", []string{"x"})
	utils.AssertEq(res, "", "c1 delete x")

	res, err := c1.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c1 sees no x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c1 sees no x")

	res, err = c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 sees no x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 sees no x")
}

func TestReadCommitted(t *testing.T) {
	database := mvcc.NewDatabase(mvcc.ReadCommittedIsolation)
	c1 := database.NewConnection()
	c1.MustExecCommand("begin", nil)

	c2 := database.NewConnection()
	c2.MustExecCommand("begin", nil)

	// Local change is visible locally.
	c1.MustExecCommand("set", []string{"x", "hey"})

	res := c1.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c1 get x")

	// Update not available to this transaction since this is not
	// committed.
	res, err := c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	c1.MustExecCommand("commit", nil)

	// Now that it's been committed, it's visible in c2.
	res = c2.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c2 get x")

	c3 := database.NewConnection()
	c3.MustExecCommand("begin", nil)

	// Local change is visible locally.
	c3.MustExecCommand("set", []string{"x", "yall"})

	res = c3.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "yall", "c3 get x")

	// But not on the other commit, again.
	res = c2.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c2 get x")

	c3.MustExecCommand("rollback", nil)

	// And still not, if the other transaction rolledback.
	res = c2.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c2 get x")

	// And if we delete it, it should show up deleted locally.
	c2.MustExecCommand("delete", []string{"x"})

	res, err = c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	c2.MustExecCommand("commit", nil)

	// It should also show up as deleted in new transactions now
	// that it has been committed.
	c4 := database.NewConnection()
	c4.MustExecCommand("begin", nil)

	res, err = c4.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c4 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c4 get x")
}

func TestRepeatableRead(t *testing.T) {
	database := mvcc.NewDatabase(mvcc.RepeatableReadIsolation)

	c1 := database.NewConnection()
	c1.MustExecCommand("begin", nil)

	c2 := database.NewConnection()
	c2.MustExecCommand("begin", nil)

	// local change is visible locally
	c1.MustExecCommand("set", []string{"x", "hey"})
	res := c1.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c1 get x")

	// update not available to this transaction since it is not committed
	res, err := c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	c1.MustExecCommand("commit", nil)

	// even after committing the update isn't visible because c1 was in-progress when c2 began
	res, err = c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	// but is available in a new transaction
	c3 := database.NewConnection()
	c3.MustExecCommand("begin", nil)

	res = c3.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c3 get x")

	// local change is visible locally
	c3.MustExecCommand("set", []string{"x", "yall"})
	res = c3.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "yall", "c3 get x")

	// But not on the other connection, again.
	res, err = c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	c3.MustExecCommand("rollback", nil)

	// And still not, regardless of rollback, because it's an older
	// transaction.
	res, err = c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c2 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c2 get x")

	// And again the rollbacked set is still not on a new transaction.
	c4 := database.NewConnection()
	c4.MustExecCommand("begin", nil)

	res = c4.MustExecCommand("get", []string{"x"})
	utils.AssertEq(res, "hey", "c4 get x")

	c4.MustExecCommand("delete", []string{"x"})
	c4.MustExecCommand("commit", nil)

	// But the delete is visible to new transactions now that this
	// has been committed.
	c5 := database.NewConnection()
	c5.MustExecCommand("begin", nil)

	res, err = c5.ExecCommand("get", []string{"x"})
	utils.AssertEq(res, "", "c5 get x")
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c5 get x")
}

// Snapshot Isolation shares all the same visibility rules as Repeatable Read, the tests get to be a little simpler!
// We'll simply test that two transactions attempting to commit a write to the same key fail. Or specifically: that the second transaction cannot commit.
func TestSnapshotIsolation(t *testing.T) {
	database := mvcc.NewDatabase(mvcc.SnapshotIsolation)

	c1 := database.NewConnection()
	c1.MustExecCommand("begin", nil)

	c2 := database.NewConnection()
	c2.MustExecCommand("begin", nil)

	c3 := database.NewConnection()
	c3.MustExecCommand("begin", nil)

	c1.MustExecCommand("set", []string{"x", "hey"})
	c1.MustExecCommand("commit", nil)

	c2.MustExecCommand("set", []string{"x", "hey"})

	res, err := c2.ExecCommand("commit", nil)
	utils.AssertEq(res, "", "c2 commit")
	utils.AssertEq(err.Error(), "write-write conflict", "c2 commit")

	// But unrelated keys cause no conflict.
	c3.MustExecCommand("set", []string{"y", "no conflict"})
	c3.MustExecCommand("commit", nil)
}

func TestSerializableIsolation(t *testing.T) {
	database := mvcc.NewDatabase(mvcc.SerializableIsolation)

	c1 := database.NewConnection()
	c1.MustExecCommand("begin", nil)

	c2 := database.NewConnection()
	c2.MustExecCommand("begin", nil)

	c3 := database.NewConnection()
	c3.MustExecCommand("begin", nil)

	c1.MustExecCommand("set", []string{"x", "hey"})
	c1.MustExecCommand("commit", nil)

	_, err := c2.ExecCommand("get", []string{"x"})
	utils.AssertEq(err.Error(), "cannot get key that doesn't exist", "c5 get x")

	res, err := c2.ExecCommand("commit", nil)
	utils.AssertEq(res, "", "c2 commit")
	utils.AssertEq(err.Error(), "read-write or write-write conflict", "c2 commit")

	// But unrelated keys cause no conflict.
	c3.MustExecCommand("set", []string{"y", "no conflict"})
	c3.MustExecCommand("commit", nil)
}
