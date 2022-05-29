package externserve

import (
	"context"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/dchest/siphash"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/chmembership"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/lookup"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
)

type LocalServerData struct {
	membership *chmembership.Membership
}

// DNSRequestHash is the hash function used to distribute keys/members uniformly.
type DNSRequestHash struct{}

// Keys to SipHash, generated at random.
// Ideally this should be decided at runtime and stored in Zookeeper.
const (
	siphash_k0 uint64 = 16038536194969526240
	siphash_k1 uint64 = 11178038365157218530
)

func (h DNSRequestHash) Sum64(data []byte) uint64 {
	return siphash.Hash(siphash_k0, siphash_k1, data)
}

// CreateConsistentHashing creates a new consistent instance.
func CreateConsistentHashing() *consistent.Consistent {
	cfg := consistent.Config{
		PartitionCount:    3,
		ReplicationFactor: 3,
		Load:              1.50,
		Hasher:            DNSRequestHash{},
	}
	return consistent.New(nil, cfg)
}

func ListenAndServeUDP(addr string, localServerData *LocalServerData) {
	s := &dns.Server{
		Addr: addr,
		Net:  "udp",
	}

	dns.HandleFunc(".", func(rw dns.ResponseWriter, queryMsg *dns.Msg) {
		log.Infof("received request from %v", rw.RemoteAddr())
		/**
		 * lookup values
		 */
		output := make(chan types.TypedResourceRecord)

		// Look up queries in parallel.
		go func(output chan<- types.TypedResourceRecord) {
			var wg sync.WaitGroup
			for _, query := range queryMsg.Question {
				wg.Add(1)
				query := query
				go func() {
					defer wg.Done()
					recs := lookup.Lookup(
						context.Background(),
						localServerData.membership,
						query,
						lookup.ExternalProfile,
					)
					for _, rec := range recs {
						output <- rec
					}
				}()
			}
			wg.Wait()
			close(output)
		}(output)

		// Collect output from each query.
		var answerResourceRecords []dns.RR
		var authorityResourceRecords []dns.RR
		var additionalResourceRecords []dns.RR
		for rec := range output {
			switch rec.Type {
			case types.ResourceAnswer:
				answerResourceRecords = append(answerResourceRecords, rec.Record)
			case types.ResourceAuthority:
				authorityResourceRecords = append(authorityResourceRecords, rec.Record)
			case types.ResourceAdditional:
				additionalResourceRecords = append(additionalResourceRecords, rec.Record)
			}
		}

		/**
		 * write response
		 */
		response := new(dns.Msg)
		response.SetReply(queryMsg)
		response.RecursionAvailable = true

		for _, answerResourceRecord := range answerResourceRecords {
			response.Answer = append(response.Answer, answerResourceRecord)
		}

		for _, authorityResourceRecord := range authorityResourceRecords {
			response.Ns = append(response.Ns, authorityResourceRecord)
		}

		for _, additionalResourceRecord := range additionalResourceRecords {
			response.Extra = append(response.Extra, additionalResourceRecord)
		}

		err := rw.WriteMsg(response)
		if err != nil {
			log.Errorf("error writing response %v", err)
		}
	})

	log.Infof("externserve: starting at %s", s.Addr)
	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("Error resolving UDP address: %v", err)
	}
}

func Start(addr string, membership *chmembership.Membership) {
	// TODO: integrate this with Zookeeper layer
	localServerData := LocalServerData{
		membership: membership,
	}

	ListenAndServeUDP(addr, &localServerData)
}
