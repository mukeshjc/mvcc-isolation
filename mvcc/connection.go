package mvcc

import "fmt"

// final bit of scaffolding we'll set up is an abstraction for database connections. A connection will have at most associated one transaction.
// users must ask the database for a new connection. Then within the connection they can manage a transaction.
type Connection struct {
	tx *Transaction
	db *Database
}

func (c *Connection) execCommand(command string, args []string) (string, error) {
	debug(command, args)

	// TODO
	return "", fmt.Errorf("unimplemented")
}

func (c *Connection) mustExecCommand(cmd string, args []string) string {
	res, err := c.execCommand(cmd, args)
	assertEq(err, nil, "unexpected error")
	return res
}

func (d *Database) newConnection() *Connection {
	return &Connection{
		db: d,
		tx: nil,
	}
}
