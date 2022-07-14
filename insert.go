//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Index mutation due to {key,docid,value} insert.
package btree

func (ln *lnode) insert(store *Store, key Key, v Value, mv *MV) (
	Node, int64, int64) {

	index, kfpos, dfpos := ln.searchGE(store, key, true)
	if kfpos >= 0 && dfpos >= 0 {
		ln.ks[index], ln.ds[index] = kfpos, dfpos
		ln.vs[index] = store.valueOf(v)
	} else {
		ln.ks = ln.ks[:len(ln.ks)+1]         // Make space in the key array
		ln.ds = ln.ds[:len(ln.ds)+1]         // Make space in the key array
		copy(ln.ks[index+1:], ln.ks[index:]) // Shift existing data out of the way
		copy(ln.ds[index+1:], ln.ds[index:]) // Shift existing data out of the way
		ln.ks[index], ln.ds[index] = store.keyOf(key, kfpos, dfpos)

		ln.vs = ln.vs[:len(ln.vs)+1]         // Make space in the value array
		copy(ln.vs[index+1:], ln.vs[index:]) // Shift existing data out of the way
		ln.vs[index] = store.valueOf(v)
	}

	ln.size = len(ln.ks)
	if ln.size <= store.maxKeys() {
		return nil, -1, -1
	}
	spawnKn, mkfpos, mdfpos := ln.split(store)
	mv.commits[spawnKn.fpos] = spawnKn
	return spawnKn, mkfpos, mdfpos
}

func (in *inode) insert(store *Store, key Key, v Value, mv *MV) (
	Node, int64, int64) {

	index, _, _ := in.searchGE(store, key, true)
	// Copy on write
	stalechild := store.FetchMVCache(in.vs[index])
	child := stalechild.copyOnWrite(store)
	mv.stales = append(mv.stales, stalechild.getLeafNode().fpos)
	mv.commits[child.getLeafNode().fpos] = child

	// Recursive insert
	spawn, mkfpos, mdfpos := child.insert(store, key, v, mv)
	in.vs[index] = child.getLeafNode().fpos
	if spawn == nil {
		return nil, -1, -1
	}

	in.ks = in.ks[:len(in.ks)+1]         // Make space in the key array
	in.ds = in.ds[:len(in.ds)+1]         // Make space in the key array
	copy(in.ks[index+1:], in.ks[index:]) // Shift existing data out of the way
	copy(in.ds[index+1:], in.ds[index:]) // Shift existing data out of the way
	in.ks[index], in.ds[index] = mkfpos, mdfpos

	in.vs = in.vs[:len(in.vs)+1]           // Make space in the value array
	copy(in.vs[index+2:], in.vs[index+1:]) // Shift existing data out of the way
	in.vs[index+1] = spawn.getLeafNode().fpos

	in.size = len(in.ks)
	max := store.maxKeys()
	if in.size <= max {
		return nil, -1, -1
	}

	// this node is full, so we have to split
	spawnIn, mkfpos, mdfpos := in.split(store)
	mv.commits[spawnIn.fpos] = spawnIn
	return spawnIn, mkfpos, mdfpos
}

// Split the leaf node into two.
//
// Before:                       |  After:
//          keys        values   |           keys        values
// newkn     0            0      |  newkn    max/2      max/2 + 1
// kn       max+1       max+2    |  kn     max/2 + 1    max+2 + 2 (0 appended)
//
// `kn` will contain the first half, while `newkn` will contain the second
// half. Returns,
//  - new leaf node,
//  - key, that splits the two nodes with CompareLess() method.
func (ln *lnode) split(store *Store) (*lnode, int64, int64) {
	// Get a free block
	max := store.maxKeys() // always even

	newkn := (&lnode{}).newNode(store) // Fetch a newnode from freelist

	copy(newkn.ks, ln.ks[max/2+1:])
	copy(newkn.ds, ln.ds[max/2+1:])
	ln.ks = ln.ks[:max/2+1]
	ln.ds = ln.ds[:max/2+1]
	ln.size = len(ln.ks)
	newkn.size = len(newkn.ks)

	copy(newkn.vs, ln.vs[max/2+1:])
	ln.vs = append(ln.vs[:max/2+1], 0)
	return newkn, newkn.ks[0], newkn.ds[0]
}

// Split intermediate node into two.
//
// Before:                       |  After:
//          keys        values   |           keys        values
// newkn     0            0      |  newkn    max/2      max/2 + 1
// kn       max+1       max+2    |  kn       max/2      max+2 + 2 (0 appended)
//
// `kn` will contain the first half, while `newkn` will contain the second
// half. Returns,
//  - new leaf node,
//  - key, that splits the two nodes with CompareLess() method.
func (in *inode) split(store *Store) (*inode, int64, int64) {
	// Get a free block
	max := store.maxKeys() // always even

	newin := (&inode{}).newNode(store) // Fetch a newnode from freelist

	copy(newin.ks, in.ks[max/2+1:])
	copy(newin.ds, in.ds[max/2+1:])
	mkfpos, mdfpos := in.ks[max/2], in.ds[max/2]
	in.ks = in.ks[:max/2]
	in.ds = in.ds[:max/2]
	in.size = len(in.ks)
	newin.size = len(newin.ks)

	copy(newin.vs, in.vs[max/2+1:])
	in.vs = in.vs[:max/2+1]
	return newin, mkfpos, mdfpos
}

func (store *Store) keyOf(k Key, kfpos, dfpos int64) (int64, int64) {
	if kfpos < 0 {
		kfpos = store.appendKey(k.Bytes())
	}
	dfpos = store.appendDocid(k.Docid())
	return kfpos, dfpos
}

func (store *Store) valueOf(v Value) int64 {
	return store.appendValue(v.Bytes())
}
