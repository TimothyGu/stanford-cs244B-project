package externserve

import (
	"net"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/miekg/dns"
	log "github.com/sirupsen/logrus"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/lookup"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
)

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
				// TODO: redo when zookeeper layer is working
				// var assignedServerNode ServerNode
				// assignedServerNode = localServerData.serverNodes[localServerData.c.LocateKey([]byte(query.Name)).String()]
				// externalServer := assignedServerNode.ipAddr + ":" + assignedServerNode.portNum
				go lookup.Lookup(&wg, query, output, "")
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

func Start(addr string) {
	lookup.InitDB()

	// TODO: integrate this with Zookeeper layer
	localServerData := LocalServerData{
		serverNodes: SetupServers(),
	}

	localServerData.c = CreateConsistentHashing()
	for _, n := range localServerData.serverNodes {
		localServerData.c.Add(n)
	}

	ListenAndServeUDP(addr, &localServerData)
}
