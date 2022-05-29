package main

import (
	"context"
	"flag"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/raftcache"
)

var (
	address = flag.String("addr", "127.0.0.1:9021", "server node address")
	get     = flag.String("get", "", "cache key to get")
	set     = flag.String("set", "", "cache key to set")
	value   = flag.String("value", "", "cache value to set (delete by default)")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, *address,
		grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	client := pb.NewRaftCacheClient(conn)

	var req pb.RaftCacheRequest
	if *get != "" {
		req.Action = pb.RaftCacheRequest_LOOKUP
		req.Key = *get
	} else if *set != "" {
		req.Key = *set
		if *value == "" {
			req.Action = pb.RaftCacheRequest_DELETE
		} else {
			req.Action = pb.RaftCacheRequest_STORE
			req.Value = *value
		}
	} else {
		log.Fatalln("need either -get or -set")
	}

	res, err := client.Cache(ctx, &req)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("response: %s", res)
}
