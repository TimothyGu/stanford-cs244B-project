package internserve

import (
	"context"
	"net"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/cache"
	pb "go.timothygu.me/stanford-cs244b-project/internal/pkg/internapi"
)

type InternAPIServer struct {
	pb.UnimplementedInternAPIServer
	c *cache.Cache
}

var _ pb.InternAPIServer = (*InternAPIServer)(nil)

func (s *InternAPIServer) NewContent(ctx context.Context, req *pb.NewContentRequest) (*emptypb.Empty, error) {
	for _, datum := range req.GetContent() {
		key := cache.Key{
			DomainName: datum.GetQuestion().GetName(),
			Type:       uint16(datum.GetQuestion().GetQtype()),
		}
		var value []cache.Value
		for _, res := range datum.GetResponse() {
			expiry := res.GetExpiry().AsTime()
			rr, _, err := dns.UnpackRR(res.GetRr(), 0)
			if err != nil {
				log.Warnf("internapi: UnpackRR: %v", err)
				continue
			}
			value = append(value, cache.Value{
				Type:   res.GetType().As(),
				Record: rr,
				Expiry: expiry,
			})
		}
		if len(value) != 0 {
			s.c.Add(key, value)
		}
	}
	return new(emptypb.Empty), nil
}

func (s *InternAPIServer) InternalListKeys(ctx context.Context, _ *emptypb.Empty) (*pb.ListKeysResponse, error) {
	res := new(pb.ListKeysResponse)
	kk := s.c.Keys()
	for _, k := range kk {
		res.Questions = append(res.Questions, &pb.DNSQuestion{
			Name:  k.DomainName,
			Qtype: uint32(k.Type),
		})
	}
	return res, nil
}

func Start(addr string, c *cache.Cache) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panicln(err)
	}

	s := &InternAPIServer{
		c: c,
	}
	grpcServer := grpc.NewServer()
	pb.RegisterInternAPIServer(grpcServer, s)
	log.Infof("internserve: starting at %v", addr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Panicln(err)
	}
}
