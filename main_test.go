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
