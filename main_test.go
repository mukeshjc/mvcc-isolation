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
