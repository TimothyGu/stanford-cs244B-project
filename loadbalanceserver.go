package main

import (
	"flag"
	"fmt"
	"net"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internserve"
)

var useConsistentHashing = flag.Bool("ch", true, "use consistent hashing to distribute requests")
var useLocalCache = flag.Bool("lc", true, "check local cache before sending requests to servers")

var basePort = flag.Int("port", 1058, "base port number (DNS = base, internapi = base+1)")

// ServerNode contains the information for server code.
type ServerNode struct {
	hashString string
	serverConn *net.UDPConn
	ipAddr     string
	portNum    string
}

func (n ServerNode) String() string {
	return n.hashString
}

type LocalServerData struct {
	serverNodes map[string]ServerNode
	c           *consistent.Consistent
}

var localServerData LocalServerData

// DNSRequestHash is the hash function used to distribute keys/members uniformly.
type DNSRequestHash struct{}

func (h DNSRequestHash) Sum64(data []byte) uint64 {
	// Used the default one provided by the consistent package.
	// TODO: Test if this hash function can provide uniformity.
	return xxhash.Sum64(data)
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

func SetupServers() map[string]ServerNode {
	m := make(map[string]ServerNode)
	serverNode1 := ServerNode{
		hashString: "node1",
		ipAddr:     "13.56.11.133",
		portNum:    "1053",
	}
	m[serverNode1.hashString] = serverNode1
	serverNode2 := ServerNode{
		hashString: "node2",
		ipAddr:     "13.56.11.133",
		portNum:    "1053",
	}
	m[serverNode2.hashString] = serverNode2
	return m
}

func ListenAndServeUDP(localServerData *LocalServerData) {

	s := &dns.Server{
		Addr: ":8083",
		Net:  "udp",
	}

	dns.HandleFunc(".", func(rw dns.ResponseWriter, queryMsg *dns.Msg) {
		log.Infof("received request from %v", rw.RemoteAddr())
		/**
		 * lookup values
		 */
		output := make(chan TypedResourceRecord)

		// Look up queries in parallel.
		go func(output chan<- TypedResourceRecord) {
			var wg sync.WaitGroup
			for _, query := range queryMsg.Question {
				wg.Add(1)
				var assignedServerNode ServerNode
				if *useConsistentHashing {
					assignedServerNode = localServerData.serverNodes[localServerData.c.LocateKey([]byte(query.Name)).String()]
				}
				externalServer := assignedServerNode.ipAddr + ":" + assignedServerNode.portNum
				go Lookup(&wg, query, output, externalServer)
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
			case ResourceAnswer:
				answerResourceRecords = append(answerResourceRecords, rec.Record)
			case ResourceAuthority:
				authorityResourceRecords = append(authorityResourceRecords, rec.Record)
			case ResourceAdditional:
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

	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("Error resolving UDP address: %v", err)
	}
}

func main() {
	InitDB()

	localServerData := LocalServerData{
		serverNodes: SetupServers(),
	}

	if *useConsistentHashing {
		localServerData.c = CreateConsistentHashing()
		for _, n := range localServerData.serverNodes {
			localServerData.c.Add(n)
		}
	}

	go ListenAndServeUDP(&localServerData)

	internAddr := fmt.Sprintf("0.0.0.0:%d", *basePort+1)

	// TODO: this should be moved
	cache := sync.Map{}
	go internserve.Start(internAddr, &cache)

	select {} // block forever
}
