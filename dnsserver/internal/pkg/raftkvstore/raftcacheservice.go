package raftkvstore

import (
	"go.etcd.io/etcd/raft/raftpb"
	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/raftcache"
)

type RaftCacheServer struct {
	pb.UnimplementedRaftCacheServer
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}

func (s *RaftCacheServer) Lookup(req *pb.RaftCacheRequest) *pb.RaftCacheReply {
	action, key := req.Action, req.Key
	switch action {
	case pb.RaftCacheRequest_LOOKUP:
		value, ok := s.store.kvStore.Load(key)
		return &pb.RaftCacheReply{Ok: ok, Value: value.(string)}
	}
}
