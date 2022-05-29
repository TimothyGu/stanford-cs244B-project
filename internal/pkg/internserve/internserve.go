package internserve

import (
	"context"
	"net"
	"sync"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/cache"
	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/internapi"
)

type Cache = sync.Map

type InternAPIServer struct {
	pb.UnimplementedInternAPIServer
	c *Cache
}

var _ pb.InternAPIServer = (*InternAPIServer)(nil)

func (s *InternAPIServer) NewContent(ctx context.Context, req *pb.NewContentRequest) (*emptypb.Empty, error) {
	for _, datum := range req.GetContent() {
		key := cache.CacheKey{
			DomainName: datum.GetQuestion().GetName(),
			Type:       uint16(datum.GetQuestion().GetQtype()),
		}
		var value []cache.CacheValue
		for _, res := range datum.GetResponse() {
			expiry := res.GetExpiry().AsTime()
			rr, _, err := dns.UnpackRR(res.GetRr(), 0)
			if err != nil {
				log.Warnf("internapi: UnpackRR: %v", err)
				continue
			}
			value = append(value, cache.CacheValue{
				Type:   res.GetType().As(),
				Record: rr,
				Expiry: expiry,
			})
		}
		if len(value) != 0 {
			s.c.Store(key, value)
		}
	}
	return new(emptypb.Empty), nil
}

func (s *InternAPIServer) InternalListKeys(ctx context.Context, _ *emptypb.Empty) (*pb.ListKeysResponse, error) {
	res := new(pb.ListKeysResponse)
	s.c.Range(func(key, value any) bool {
		ck := key.(cache.CacheKey)
		res.Questions = append(res.Questions, &pb.DNSQuestion{
			Name:  ck.DomainName,
			Qtype: uint32(ck.Type),
		})
		return true
	})
	return res, nil
}

func Start(addr string, c *Cache) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}

	s := &InternAPIServer{
		c: c,
	}
	grpcServer := grpc.NewServer()
	pb.RegisterInternAPIServer(grpcServer, s)
	log.Infof("internserve: started at %v", addr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Panicln(err)
	}
}
