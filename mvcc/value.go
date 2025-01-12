package mvcc

// a value in the database will be defined with start and end transaction ids.
type Value struct {
	txStartId uint64
	txEndId   uint64
	value     string
}
