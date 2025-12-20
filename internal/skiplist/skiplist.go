package skiplist

import (
	"bytes"
	"math/rand"
	"sync"
	"time"
)

const (
	// Maximum level for the skip list
	maxLevel = 12
	// Coin flip probability for level generation
	// Each new node has a probability of 0.25 to increase its level
	probability = 0.25
)

// A node in the skip list
// One key will only have a single node associated with it
// forward[i] points to the next node at level i
// Ex: if key 15 is at level 2, forward[0] points to the next node at level 0 (base level)
// and forward[1] points to the next node at level 1, skipping some nodes in between
type node struct {
	key     []byte
	value   []byte
	forward []*node
}

// SkipList is a probabilistic data structure that allows
// fast search, insertion, and deletion operations
// by maintaining multiple levels of linked lists
// for efficient traversal.
// level 3:  H -------------------------------------------------> 42
// level 2:  H -----------------> 15 --------------------------> 42
// level 1:  H ----> 7 --------> 15 -----> 20 ----------------> 42
// level 0:  H -> 3 -> 7 -> 15 -> 20 -> 42
type SkipList struct {
	head  *node
	level int
	rand  *rand.Rand
	mutex sync.RWMutex
}

// NewSkipList creates a new empty SkipList instance
func NewSkipList() *SkipList {
	return &SkipList{
		// head is the entrypoint of the skip list
		// contains the pointer to first node in all the levels
		head:  &node{forward: make([]*node, maxLevel)},
		level: 1,
		rand:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Insert inserts a key-value pair into the skip list
// If the key already exists, its value is updated
func (s *SkipList) Insert(key, value []byte) {
	// Get the lock for writing
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// As we search down through the levels, we need to remember
	// the last node we visited at each level
	// This is because when we insert a new node, we need to update
	// the forward pointers of these nodes to our new node
	update := make([]*node, maxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// check if the key already exists, if so update its value
	target := current.forward[0]
	if target != nil && bytes.Equal(target.key, key) {
		target.value = value
		return
	}

	// get the level for the new node
	lvl := s.randomLevel()
	// If the new level is greater than the current highest level
	// we need to update the update array for the new levels.
	// the forward pointers of the head between levels s.level and lvl - 1
	// will point to the new node
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	newNode := &node{
		key:     key,
		value:   value,
		forward: make([]*node, lvl),
	}

	// Insert the new node by updating the forward pointers
	// for all the nodes that came before it at each level
	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
}

// Get retrieves the value associated with the given key
// Returns the value and true if found, otherwise nil and false
func (s *SkipList) Get(key []byte) ([]byte, bool) {
	// Get the lock for reading
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	current := s.head
	// at each level, traverse as right as possible
	// until we find a node whose key is >= the target key
	for i := s.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	// move to next node at level 0
	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, true
	}

	return nil, false
}

// Delete deletes the node with the given key from the skip list
// Returns true if the node was found and deleted, otherwise false
func (s *SkipList) Delete(key []byte) bool {
	// Get the lock for writing
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// to track the predecessor nodes at each level
	// when we delete a node, we need to update their forward pointers
	// to bypass the deleted node
	update := make([]*node, maxLevel)
	current := s.head

	for i := s.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// target is the candidate node to delete
	target := current.forward[0]
	if target == nil || !bytes.Equal(target.key, key) {
		return false
	}

	// Update forward pointers to bypass the target node
	for i := 0; i < s.level; i++ {
		if update[i].forward[i] != target {
			break
		}
		update[i].forward[i] = target.forward[i]
	}

	// Decrease the level of the skip list if necessary
	for s.level > 1 && s.head.forward[s.level-1] == nil {
		s.level--
	}

	return true
}

// randomLevel gets a random level for a new node
// Each level has a probability of 0.25 to be added
func (s *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rand.Float64() < probability {
		lvl++
	}
	return lvl
}

// NewIterator creates a new iterator for the skip list
func (s *SkipList) NewIterator() *Iterator {
	it := &Iterator{list: s}
	it.SeekToFirst()
	return it
}
