// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Modified to be used in conjunction with our DNS server code

package raftkvstore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Key = string
type Value = string

// a key-value store backed by raft
type KVStore struct {
	proposeC    chan<- []byte // channel for proposing updates
	mu          sync.RWMutex  // reader-writer lock that guards the kvStore pointer
	kvStore     *sync.Map     // current committed key-value pairs.
	snapshotter *snap.Snapshotter
}

type kvops int

const (
	Add kvops = iota
	Delete
)

type kv struct {
	Key Key
	Val Value
	Ops kvops
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- []byte, commitC <-chan *commit, errorC <-chan error) *KVStore {
	s := &KVStore{proposeC: proposeC, kvStore: new(sync.Map), snapshotter: snapshotter}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into KVStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

// Load returns the value in the store for a key, or nil if no
// value is present.
// The ok result indicates whether value was found in the store.
func (s *KVStore) Lookup(key Key) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore.Load(key)
	return v, ok
}

func (s *KVStore) Propose(k Key, v Value) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v, Add}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.Bytes()
}

func (s *KVStore) ProposeDelete(k Key) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, "", Delete}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.Bytes()
}

func (s *KVStore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	for commit := range commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewReader(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Printf("raftkvstore: could not decode message (%v)", err)
				continue
			}

			// We only need a read lock here to make sure no one is updating the kvStore pointer.
			s.mu.RLock()
			if dataKv.Ops == Add {
				s.kvStore.Store(dataKv.Key, dataKv.Val)
			} else if dataKv.Ops == Delete {
				s.kvStore.Delete(dataKv.Key)
			} else {
				log.Printf("raftkvstore: unknown kv operation %v", dataKv.Ops)
			}
			s.mu.RUnlock()
		}

		close(commit.applyDoneC)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *KVStore) storeToMap() map[any]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m := map[any]any{}
	s.kvStore.Range(func(k any, v any) bool {
		m[k] = v
		return true
	})
	return m
}

func (s *KVStore) GetSnapshot() ([]byte, error) {
	return json.Marshal(s.storeToMap())
}

func (s *KVStore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *KVStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[Key]Value
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}

	newKvStore := new(sync.Map)

	for k, v := range store {
		newKvStore.Store(k, v)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = newKvStore
	return nil
}
