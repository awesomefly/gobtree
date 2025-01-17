//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"bytes"
	"github.com/awesomefly/gobtree"
	"log"
	"os"
	"time"
)

var conf = btree.Config{
	Idxfile: "./data/test_insread_index2.dat",
	Kvfile:  "./data/test_insread_kv2.dat",
	IndexConfig: btree.IndexConfig{
		Sectorsize: 512,
		Flistsize:  1000 * btree.OFFSET_SIZE,
		Blocksize:  512,
	},
	Maxlevel:      6,
	RebalanceThrs: 3,
	AppendRatio:   0.7,
	DrainRate:     200,
	MaxLeafCache:  1000,
	Sync:          false,
	Nocache:       false,
	//Debug: true,
}

func main() {
	//os.Remove(conf.Idxfile)
	//os.Remove(conf.Kvfile)
	if conf.Debug {
		fd, _ := os.Create("debug")
		log.SetOutput(fd)
	}

	bt := btree.NewBTree(btree.NewStore(conf))
	factor := 1
	count := 20
	//seed := time.Now().UnixNano()
	seed := 1637823047798425000

	log.Println("Seed:", seed)
	keys, values := btree.TestData(10000, int64(seed))
	log.Println(time.Now())
	for i := 0; i < factor; i++ {
		for j := 0; j < count; j++ {
			k, v := keys[j], values[j]
			k.Id = int64((i * count) + j)
			log.Printf("insert key:%s, val:%s\n", k.K, v.V)
			//bt.Insert(k, v)
		}
		log.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
	}

	dirty := true // if drain then dirty false
	//bt.Drain()

	//countIn(bt, count, factor)
	//front(bt)
	//keyset(bt, count, factor)
	//fullset(bt, count, factor)
	//containsEquals(bt, count, factor, keys)
	lookup(bt, count, factor, keys, values, dirty)
}

func countIn(bt *btree.BTree, count int, factor int) {
	fullcount := count * factor
	log.Println("count")
	if bt.Count() != int64(fullcount) {
		panic("Count mismatch")
	}
}

func front(bt *btree.BTree) {
	frontK, frontD, frontV := bt.Front()
	log.Println("front --", string(frontK), string(frontD), string(frontV))
}

func keyset(bt *btree.BTree, count, factor int) {
	log.Println("KeySet")
	fullcount := count * factor
	frontK, _, _ := bt.Front()
	ch := bt.KeySet()
	prev, kcount := <-ch, 1
	if bytes.Compare(prev, frontK) != 0 {
		panic("Front key does not match")
	}
	for {
		key := <-ch
		if key == nil {
			break
		}
		if bytes.Compare(prev, key) == 1 {
			panic("Not sorted")
		}
		prev = key
		kcount += 1
	}
	if kcount != fullcount {
		panic("KeySet does not return full keys")
	}
}

func fullset(bt *btree.BTree, count, factor int) {
	log.Println("FullSet")
	fullcount := count * factor
	frontK, _, _ := bt.Front()
	ch := bt.FullSet()
	prevKey, prevDocid, _, kcount := <-ch, <-ch, <-ch, 1
	if bytes.Compare(prevKey, frontK) != 0 {
		panic("Front key does not match")
	}
	for {
		key := <-ch
		if key == nil {
			break
		}
		docid, val := <-ch, <-ch
		if bytes.Compare(prevKey, key) == 1 {
			panic("Not sorted")
		}
		if bytes.Equal(prevKey, key) && bytes.Compare(prevDocid, docid) == 1 {
			panic("Not sorted")
		}
		prevKey, prevDocid, _ = key, docid, val
		kcount += 1
	}
	if kcount != fullcount {
		panic("FullSet does not return full keys")
	}
}

func containsEquals(bt *btree.BTree, count, factor int, keys []*btree.TestKey) {
	log.Println("Contains Equals")
	for i := 0; i < factor; i++ {
		for j := 0; j < count; j++ {
			key := *keys[j]
			key.Id = int64((i * count) + j)
			if bt.Equals(&key) == false {
				panic("Does not equal key")
			}
			if bt.Contains(&key) == false {
				panic("Does not contain key")
			}
			key.Id = -1000
			if bt.Equals(&key) == true {
				panic("Does not expect key")
			}
		}
	}
}

func lookup(bt *btree.BTree, count, factor int, keys []*btree.TestKey,
	values []*btree.TestValue, dirty bool) {

	log.Println("Lookup")
	for i := 0; i < count; i++ {
		//log.Printf("key:%s\n", keys[i].K)
		keys[i].Id = 0
		var ch chan []byte
		if dirty {
			ch = bt.LookupDirty(keys[i])
		} else {
			ch = bt.Lookup(keys[i])
		}
		vals := make([]string, 0)
		for {
			x := <-ch
			log.Printf("key:%s, val:%s\n", keys[i].K, string(x))
			if x == nil {
				log.Println("read nil")
				break
			}
			vals = append(vals, string(x))
		}
		//sort.Strings(vals)
		if len(vals) == 0 || vals[0] != values[i].V {
			println("Lookup value mismatch")
			return
		}

	}
	log.Println("End Lookup")

	//bt.Drain()
	//time.Sleep(1*time.Second)
}
