package memtable

import "github.com/ritik/limedb/internal/skiplist"

type Memtable struct {
	skiplist *skiplist.SkipList
}

func NewMemtable() *Memtable {
	return &Memtable{
		skiplist: skiplist.NewSkipList(),
	}
}

// Put inserts a MemtableEntry into the Memtable
func (m *Memtable) Put(entry *MemtableEntry) {

}
