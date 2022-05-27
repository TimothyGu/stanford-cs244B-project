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

package main

import (
	"flag"
	"fmt"
	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/raftcache"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/raftkvstore"
	"google.golang.org/grpc"
	"log"
	"net"
	"strings"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	cluster = flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id      = flag.Int("id", 1, "node ID")
	kvport  = flag.Int("port", 9121, "key-value server port")
	join    = flag.Bool("join", false, "join an existing cluster")
)

func main() {
	flag.Parse()

	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan *raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *raftkvstore.KVStore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := raftkvstore.NewRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = raftkvstore.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", *kvport))
	if err != nil {
		log.Panicln(err)
	}

	grpcServer := grpc.NewServer()
	cacheServer := raftkvstore.NewRaftCacheServer(kvs, confChangeC)
	pb.RegisterRaftCacheServer(grpcServer, cacheServer)
	if err := grpcServer.Serve(listener); err != nil {
		log.Panicln(err)
	}
}
