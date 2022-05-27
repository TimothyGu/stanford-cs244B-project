package raftkvstore

import "go.etcd.io/etcd/raft/v3/raftpb"

// Handler for a http based key-value store backed by raft
type dnsKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) Lookup(key string) (string, bool) {
	v, ok := h.store.Lookup(key)
	return v, ok
}

func (h *httpKVAPI) Store(key string, value string) {
	h.store.Propose(key, value)
}

func (h *httpKVAPI) Delete(key string) {
	h.store.ProposeDelete(key)
}
