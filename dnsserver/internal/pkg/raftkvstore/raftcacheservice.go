package raftkvstore

import (
	"context"
	"log"

	"go.etcd.io/etcd/raft/v3/raftpb"
	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/raftcache"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RaftCacheServer struct {
	pb.UnimplementedRaftCacheServer
	store       *KVStore
	confChangeC chan<- *raftpb.ConfChange
}

var _ pb.RaftCacheServer = (*RaftCacheServer)(nil)

func NewRaftCacheServer(store *KVStore, confChangeC chan<- *raftpb.ConfChange) *RaftCacheServer {
	return &RaftCacheServer{store: store, confChangeC: confChangeC}
}

// "DNSRecord{\"name\":\"google.com\",\"type\":\"A\"}"

func (s *RaftCacheServer) Cache(ctx context.Context, req *pb.RaftCacheRequest) (*pb.RaftCacheReply, error) {
	action, key := req.Action, req.Key
	switch action {
	case pb.RaftCacheRequest_LOOKUP:
		value, ok := s.store.Lookup(key)

		// nil cannot be converted to string
		if !ok {
			value = ""
		}

		return &pb.RaftCacheReply{Ok: ok, Value: value.(string)}, nil
	case pb.RaftCacheRequest_STORE:
		s.store.Propose(key, req.Value)
		return &pb.RaftCacheReply{Ok: true}, nil
	case pb.RaftCacheRequest_DELETE:
		s.store.ProposeDelete(key)
		return &pb.RaftCacheReply{Ok: true}, nil
	}
	return &pb.RaftCacheReply{Ok: false}, nil
}

func (s *RaftCacheServer) ConfChange(ctx context.Context, req *pb.ConfChangeRequest) (*emptypb.Empty, error) {
	var confChange raftpb.ConfChange
	err := confChange.Unmarshal(req.Buf)
	if err != nil {
		log.Printf("confchange: unmarshal failed: %v", err)
		return nil, err
	}
	s.confChangeC <- &confChange
	return new(emptypb.Empty), nil
}
