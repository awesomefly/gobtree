//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package btree

// Create a new copy of node by assigning a free file-position to it.
func (ln *lnode) copyOnWrite(store *Store) Node {
	newkn := (&lnode{}).newNode(store)
	newkn.ks = newkn.ks[:len(ln.ks)]
	copy(newkn.ks, ln.ks)
	newkn.ds = newkn.ds[:len(ln.ds)]
	copy(newkn.ds, ln.ds)
	newkn.vs = newkn.vs[:len(ln.vs)]
	copy(newkn.vs, ln.vs)
	newkn.size = len(ln.ks)
	return newkn
}

// Refer above.
func (in *inode) copyOnWrite(store *Store) Node {
	newin := (&inode{}).newNode(store)
	newin.ks = newin.ks[:len(in.ks)]
	copy(newin.ks, in.ks)
	newin.ds = newin.ds[:len(in.ds)]
	copy(newin.ds, in.ds)
	newin.vs = newin.vs[:len(in.vs)]
	copy(newin.vs, in.vs)
	newin.size = len(in.ks)
	return newin
}

// Create a new instance of `lnode`, an in-memory representation of btree leaf
// block.
//   * `keys` slice must be half sized and zero valued, capacity of keys slice
//     must be 1 larger to accomodate overflow-detection.
//   * `values` slice must be half+1 sized and zero valued, capacity of value
//     slice must be 1 larger to accomodate overflow-detection.
func (ln *lnode) newNode(store *Store) *lnode {
	fpos := store.WStore.freelist.pop()

	max := store.maxKeys() // always even
	b := (&block{leaf: TRUE}).newBlock(max/2, max)
	newkn := &lnode{block: *b, fpos: fpos, dirty: true}
	return newkn
}

// Refer to the notes above.
func (in *inode) newNode(store *Store) *inode {
	fpos := store.WStore.freelist.pop()

	max := store.maxKeys() // always even
	b := (&block{leaf: FALSE}).newBlock(max/2, max)
	kn := lnode{block: *b, fpos: fpos, dirty: true}
	newin := &inode{lnode: kn}
	return newin
}
