package skiplist

// Iterator helps in traversing the skip list from the beginning to the end
// without exposing the internal structure of the skip list.
// Traversal is done at the base level (level 0) to access all elements in order.
type Iterator struct {
	list    *SkipList
	current *node
	locked  bool
}

// SeekToFirst seeks the iterator to the first element in the skip list
func (it *Iterator) SeekToFirst() {
	it.current = it.list.head.forward[0]
}

// Valid checks if the iterator is at a valid position
func (it *Iterator) Valid() bool {
	return it.current != nil
}

// Key returns the key of the current element the iterator is pointing to
func (it *Iterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.key
}

// Value returns the value of the current element the iterator is pointing to
func (it *Iterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.value
}

// Advances the iterator to the next element in the skip list
func (it *Iterator) Next() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}
