package main

import (
	"context"
	"flag"
	"net"
	"time"

	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internapi"
)

var (
	address = flag.String("addr", "127.0.0.1:1059", "server node address")
	set     = flag.String("set", "", "domain name to set")
)

func main() {
	flag.Parse()

	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, *address,
		grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	client := internapi.NewInternAPIClient(conn)

	var res any
	if *set != "" {
		name := dns.Fqdn(*set)
		rr := &dns.A{
			Hdr: dns.RR_Header{
				Name:   name,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
			},
			A: net.ParseIP("127.0.0.1"),
		}
		msg := make([]byte, dns.Len(rr))
		_, err = dns.PackRR(rr, msg, 0, nil, false)
		if err != nil {
			log.Fatalln(err)
		}
		req := &internapi.NewContentRequest{
			Content: []*internapi.DNSQuestionResponse{{
				Question: &internapi.DNSQuestion{
					Name:  name,
					Qtype: uint32(dns.TypeA),
				},
				Response: []*internapi.DNSResponse{{
					Type:   internapi.DNSResponse_ANSWER,
					Rr:     msg,
					Expiry: timestamppb.New(time.Now().Add(24 * time.Hour)),
				}},
			}},
		}
		res, err = client.NewContent(ctx, req)
	} else {
		res, err = client.InternalListKeys(ctx, new(emptypb.Empty))
	}
	if err != nil {
		log.Fatalln(err)
	}

	log.Print(res)
}
