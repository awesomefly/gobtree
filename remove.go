//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package btree

// Return the mutated node along with a boolean that says whether a rebalance
// is required or not.
func (ln *lnode) remove(store *Store, key Key, mv *MV) (
	Node, bool, int64, int64) {

	index, equal := ln.searchEqual(store, key)
	mk, md := int64(-1), int64(-1)
	if equal == false {
		return ln, false, mk, md
	}

	copy(ln.ks[index:], ln.ks[index+1:])
	copy(ln.ds[index:], ln.ds[index+1:])
	ln.ks = ln.ks[:len(ln.ks)-1]
	ln.ds = ln.ds[:len(ln.ds)-1]
	ln.size = len(ln.ks)

	copy(ln.vs[index:], ln.vs[index+1:])
	ln.vs = ln.vs[:len(ln.ks)+1]

	//Debug
	if len(ln.vs) != len(ln.ks)+1 {
		panic("Bomb")
	}

	// If first entry in the leaf node is always a separator key in
	// intermediate node.
	if index == 0 && ln.size > 0 {
		mk, md = ln.ks[0], ln.ds[0]
	}

	if ln.size >= store.RebalanceThrs {
		return ln, false, mk, md
	}
	return ln, true, mk, md
}

// Return the mutated node along with a boolean that says whether a rebalance
// is required or not.
func (in *inode) remove(store *Store, key Key, mv *MV) (
	Node, bool, int64, int64) {

	index, equal := in.searchEqual(store, key)

	// Copy on write
	stalechild := store.FetchMVCache(in.vs[index])
	child := stalechild.copyOnWrite(store)
	mv.stales = append(mv.stales, stalechild.getLeafNode().fpos)
	mv.commits[child.getLeafNode().fpos] = child

	// Recursive remove
	child, rebalnc, mk, md := child.remove(store, key, mv)
	if equal {
		if mk < 0 || md < 0 {
			panic("separator cannot be less than zero")
		}
		if index < 1 {
			panic("cannot be less than 1")
		}
		in.ks[index-1], in.ds[index-1] = mk, md
	}
	in.vs[index] = child.getLeafNode().fpos

	if rebalnc == false {
		return in, false, mk, md
	}

	var node Node = in

	// FIXME : In the below rebalance logic, we are fetching the left and right
	// node and discarding it if they are of other type from child. Can this
	// be avoided and optimized ?

	// Try to rebalance from left, if there is a left node available.
	if rebalnc && (index > 0) {
		left := store.FetchMVCache(in.vs[index-1])
		if canRebalance(child, left) {
			node, index = in.rebalanceLeft(store, index, child, left, mv)
		}
	}
	// Try to rebalance from right, if there is a right node available.
	if rebalnc && (index >= 0) && (index+1 <= in.size) {
		right := store.FetchMVCache(in.vs[index+1])
		if canRebalance(child, right) {
			node, index = in.rebalanceRight(store, index, child, right, mv)
		}
	}

	// There is one corner case, where node is not `in` but `child` but in is
	// in mv.commits and flushed into the disk, but actually orphaned.

	if node.getLeafNode().size >= store.RebalanceThrs {
		return node, false, mk, md
	}
	return node, true, mk, md
}

func (in *inode) rebalanceLeft(store *Store, index int, child Node, left Node, mv *MV) (
	Node, int) {

	count := left.balance(store, child)

	mk, md := in.ks[index-1], in.ds[index-1]
	if count == 0 { // We can merge with left child
		_, stalenodes := left.mergeRight(store, child, mk, md)
		mv.stales = append(mv.stales, stalenodes...)
		if in.size == 1 { // This is where btree-level gets reduced. crazy eh!
			mv.stales = append(mv.stales, in.fpos)
			return child, -1
		} else {
			// The median aka seperator has to go
			copy(in.ks[index-1:], in.ks[index:])
			copy(in.ds[index-1:], in.ds[index:])
			in.ks = in.ks[:len(in.ks)-1]
			in.ds = in.ds[:len(in.ds)-1]
			in.size = len(in.ks)
			// left-child has to go
			copy(in.vs[index-1:], in.vs[index:])
			in.vs = in.vs[:len(in.ks)+1]
			return in, (index - 1)
		}
	} else {
		mv.stales = append(mv.stales, left.getLeafNode().fpos)
		left := left.copyOnWrite(store)
		mv.commits[left.getLeafNode().fpos] = left
		in.ks[index-1], in.ds[index-1] = left.rotateRight(store, child, count, mk, md)
		in.vs[index-1] = left.getLeafNode().fpos
		return in, index
	}
}

func (in *inode) rebalanceRight(store *Store, index int, child Node, right Node, mv *MV) (
	Node, int) {

	count := right.balance(store, child)

	mk, md := in.ks[index], in.ds[index]
	if count == 0 {
		_, stalenodes := child.mergeLeft(store, right, mk, md)
		mv.stales = append(mv.stales, stalenodes...)
		if in.size == 1 { // There is where btree-level gets reduced. crazy eh!
			mv.stales = append(mv.stales, in.fpos)
			return child, -1
		} else {
			// The median aka separator has to go
			copy(in.ks[index:], in.ks[index+1:])
			copy(in.ds[index:], in.ds[index+1:])
			in.ks = in.ks[:len(in.ks)-1]
			in.ds = in.ds[:len(in.ds)-1]
			in.size = len(in.ks)
			// right child has to go
			copy(in.vs[index+1:], in.vs[index+2:])
			in.vs = in.vs[:len(in.ks)+1]
			return in, index
		}
	} else {
		mv.stales = append(mv.stales, right.getLeafNode().fpos)
		right := right.copyOnWrite(store)
		mv.commits[right.getLeafNode().fpos] = right
		in.ks[index], in.ds[index] = child.rotateLeft(store, right, count, mk, md)
		in.vs[index+1] = right.getLeafNode().fpos
		return in, index
	}
}

func (ln *lnode) balance(store *Store, to Node) int {
	max := store.maxKeys()
	size := ln.size + to.getLeafNode().size
	if float64(size) < (float64(max) * float64(0.6)) { // FIXME magic number ??
		return 0
	} else {
		return (ln.size - store.RebalanceThrs) / 2
	}
}

// Merge `kn` into `other` Node, and return,
//  - merged `other` node,
//  - `kn` as stalenode
func (ln *lnode) mergeRight(store *Store, othern Node, mk, md int64) (
	Node, []int64) {

	other := othern.(*lnode)
	max := store.maxKeys()
	if ln.size+other.size >= max {
		panic("We cannot merge knodes now. Combined size is greater")
	}
	other.ks = other.ks[:ln.size+other.size]
	other.ds = other.ds[:ln.size+other.size]
	copy(other.ks[ln.size:], other.ks[:other.size])
	copy(other.ds[ln.size:], other.ds[:other.size])
	copy(other.ks[:ln.size], ln.ks)
	copy(other.ds[:ln.size], ln.ds)

	other.vs = other.vs[:ln.size+other.size+1]
	copy(other.vs[ln.size:], other.vs[:other.size+1])
	copy(other.vs[:ln.size], ln.vs[:ln.size]) // Skip last value, which is zero
	other.size = len(other.ks)

	//Debug
	if len(other.vs) != len(other.ks)+1 {
		panic("Bomb")
	}

	store.WStore.countMergeRight += 1
	return other, []int64{ln.fpos}
}

// rotate `count` entries from `left` node to child `n` node. Return the median
func (ln *lnode) rotateRight(store *Store, n Node, count int, mk, md int64) (
	int64, int64) {

	child := n.(*lnode)
	chlen, leftlen := len(child.ks), len(ln.ks)

	// Move last `count` keys from ln -> child.
	child.ks = child.ks[:chlen+count] // First expand
	child.ds = child.ds[:chlen+count] // First expand
	copy(child.ks[count:], child.ks[:chlen])
	copy(child.ds[count:], child.ds[:chlen])
	copy(child.ks[:count], ln.ks[leftlen-count:])
	copy(child.ds[:count], ln.ds[leftlen-count:])
	// Blindly shrink ln keys
	ln.ks = ln.ks[:leftlen-count]
	ln.ds = ln.ds[:leftlen-count]
	// Update size.
	ln.size, child.size = len(ln.ks), len(child.ks)

	// Move last count values from ln -> child
	child.vs = child.vs[:chlen+count+1] // First expand
	copy(child.vs[count:], child.vs[:chlen+1])
	copy(child.vs[:count], ln.vs[leftlen-count:leftlen])
	// Blinldy shrink ln values and then append it with null pointer
	ln.vs = append(ln.vs[:leftlen-count], 0)

	//Debug
	if (len(ln.vs) != len(ln.ks)+1) || (len(child.vs) != len(child.ks)+1) {
		panic("Bomb")
	}

	// Return the median
	store.WStore.countRotateRight += 1
	return child.ks[0], child.ds[0]
}

// Merge `other` into `kn` Node, and return,
//  - merged `kn` node,
//  - `other` as stalenode
func (ln *lnode) mergeLeft(store *Store, othern Node, mk, md int64) (
	Node, []int64) {

	other := othern.(*lnode)
	max := store.maxKeys()
	if ln.size+other.size >= max {
		panic("We cannot merge knodes now. Combined size is greater")
	}
	ln.ks = ln.ks[:ln.size+other.size]
	ln.ds = ln.ds[:ln.size+other.size]
	copy(ln.ks[ln.size:], other.ks[:other.size])
	copy(ln.ds[ln.size:], other.ds[:other.size])

	ln.vs = ln.vs[:ln.size+other.size+1]
	copy(ln.vs[ln.size:], other.vs[:other.size+1])
	ln.size = len(ln.ks)

	//Debug
	if len(ln.vs) != len(ln.ks)+1 {
		panic("Bomb")
	}

	store.WStore.countMergeLeft += 1
	return ln, []int64{other.fpos}
}

// rotate `count` entries from right `n` node to `child` node. Return median
func (ln *lnode) rotateLeft(store *Store, n Node, count int, mk, md int64) (
	int64, int64) {

	right := n.(*lnode)
	chlen := len(ln.ks)

	// Move first `count` keys from right -> ln.
	ln.ks = ln.ks[:chlen+count] // First expand
	ln.ds = ln.ds[:chlen+count] // First expand
	copy(ln.ks[chlen:], right.ks[:count])
	copy(ln.ds[chlen:], right.ds[:count])
	// Don't blindly shrink right keys
	copy(right.ks, right.ks[count:])
	copy(right.ds, right.ds[count:])
	right.ks = right.ks[:len(right.ks)-count]
	right.ds = right.ds[:len(right.ds)-count]
	// Update size.
	right.size, ln.size = len(right.ks), len(ln.ks)

	// Move last count values from right -> ln
	ln.vs = ln.vs[:chlen+count+1] // First expand
	copy(ln.vs[chlen:], right.vs[:count])
	ln.vs[chlen+count] = 0
	// Don't blinldy shrink right values
	copy(right.vs, right.vs[count:])
	right.vs = right.vs[:len(right.vs)-count]

	//Debug
	if len(ln.vs) != len(ln.ks)+1 {
		panic("Bomb")
	}

	// Return the median
	store.WStore.countRotateLeft += 1
	return right.ks[0], right.ds[0]
}

// Merge `in` into `other` Node, and return,
//  - merged `other` node,
//  - `in` as stalenode
func (in *inode) mergeRight(store *Store, othern Node, mk, md int64) (
	Node, []int64) {

	other := othern.(*inode)
	max := store.maxKeys()
	if (in.size + other.size + 1) >= max {
		panic("We cannot merge inodes now. Combined size is greater")
	}
	other.ks = other.ks[:in.size+other.size+1]
	other.ds = other.ds[:in.size+other.size+1]
	copy(other.ks[in.size+1:], other.ks[:other.size])
	copy(other.ds[in.size+1:], other.ds[:other.size])
	copy(other.ks[:in.size], in.ks)
	copy(other.ds[:in.size], in.ds)
	other.ks[in.size], other.ds[in.size] = mk, md

	other.vs = other.vs[:in.size+other.size+2]
	copy(other.vs[in.size+1:], other.vs)
	copy(other.vs[:in.size+1], in.vs)
	other.size = len(other.ks)

	store.WStore.countMergeRight += 1
	return other, []int64{in.fpos}
}

// rotate `count` entries from `left` node to child `n` node. Return the median
func (in *inode) rotateRight(store *Store, n Node, count int, mk, md int64) (
	int64, int64) {

	child := n.(*inode)
	in.ks = append(in.ks, mk)
	in.ds = append(in.ds, md)
	chlen, leftlen := len(child.ks), len(in.ks)

	// Move last `count` keys from left -> child.
	child.ks = child.ks[:chlen+count] // First expand
	child.ds = child.ds[:chlen+count] // First expand
	copy(child.ks[count:], child.ks[:chlen])
	copy(child.ds[count:], child.ds[:chlen])
	copy(child.ks[:count], in.ks[leftlen-count:])
	copy(child.ds[:count], in.ds[leftlen-count:])
	// Blindly shrink left keys
	in.ks = in.ks[:leftlen-count]
	in.ds = in.ds[:leftlen-count]
	// Update size.
	in.size, child.size = len(in.ks), len(child.ks)

	// Move last count values from left -> child
	child.vs = child.vs[:chlen+count+1] // First expand
	copy(child.vs[count:], child.vs[:chlen+1])
	copy(child.vs[:count], in.vs[len(in.vs)-count:])
	in.vs = in.vs[:len(in.vs)-count]
	// Pop out median
	mk, md = in.ks[in.size-1], in.ds[in.size-1]
	in.ks = in.ks[:in.size-1]
	in.ds = in.ds[:in.size-1]
	in.size = len(in.ks)
	// Return the median
	store.WStore.countRotateRight += 1
	return mk, md
}

// Merge `other` into `in` Node, and return,
//  - merged `in` node,
//  - `other` as stalenode
func (in *inode) mergeLeft(store *Store, othern Node, mk, md int64) (
	Node, []int64) {

	other := othern.(*inode)
	max := store.maxKeys()
	if (in.size + other.size + 1) >= max {
		panic("We cannot merge inodes now. Combined size is greater")
	}
	in.ks = in.ks[:in.size+other.size+1]
	in.ds = in.ds[:in.size+other.size+1]
	copy(in.ks[in.size+1:], other.ks[:other.size])
	copy(in.ds[in.size+1:], other.ds[:other.size])
	in.ks[in.size], in.ds[in.size] = mk, md

	in.vs = in.vs[:in.size+other.size+2]
	copy(in.vs[in.size+1:], other.vs[:other.size+1])
	in.size = len(in.ks)

	store.WStore.countMergeLeft += 1
	return in, []int64{other.fpos}
}

// rotate `count` entries from right `n` node to `child` node. Return median
func (in *inode) rotateLeft(store *Store, n Node, count int, mk, md int64) (
	int64, int64) {

	right := n.(*inode)
	in.ks = append(in.ks, mk)
	in.ds = append(in.ds, md)
	chlen := len(in.ks)
	rlen := len(right.ks)

	// Move first `count` keys from right -> child.
	in.ks = in.ks[:chlen+count] // First expand
	in.ds = in.ds[:chlen+count] // First expand
	copy(in.ks[chlen:], right.ks[:count])
	copy(in.ds[chlen:], right.ds[:count])
	// Don't blindly shrink right keys
	copy(right.ks, right.ks[count:])
	copy(right.ds, right.ds[count:])
	right.ks = right.ks[:rlen-count]
	right.ds = right.ds[:rlen-count]
	// Update size.
	right.size, in.size = len(right.ks), len(in.ks)

	// Move last count values from right -> child
	in.vs = in.vs[:chlen+count] // First expand
	copy(in.vs[chlen:], right.vs[:count])
	// Don't blinldy shrink right values
	copy(right.vs, right.vs[count:])
	right.vs = right.vs[:rlen-count+1]

	// Pop out median
	mk, md = in.ks[in.size-1], in.ds[in.size-1]
	in.ks = in.ks[:in.size-1]
	in.ds = in.ds[:in.size-1]
	in.size = len(in.ks)
	// Return the median
	store.WStore.countRotateLeft += 1
	return mk, md
}

func canRebalance(n Node, m Node) bool {
	var rc bool
	if _, ok := n.(*lnode); ok {
		if _, ok = m.(*lnode); ok {
			rc = true
		}
	} else {
		if _, ok = m.(*inode); ok {
			rc = true
		}
	}
	return rc
}
