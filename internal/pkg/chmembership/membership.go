package chmembership

import (
	"sync"
	"time"

	c "github.com/buraksezer/consistent"
	"github.com/go-zookeeper/zk"
	"github.com/golang-collections/collections/set"
	log "github.com/sirupsen/logrus"

	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

const CH_MEMBERSHIP_PATH = "/chmembership"

func getAbsolutePath(relativePath string) string {
	return zkc.GetAbsolutePath(CH_MEMBERSHIP_PATH, relativePath)
}

/*
	/chmembership/servername(ip:port)
*/

type ServerNode struct {
	Name string
	Addr string
}

func (sa *ServerNode) String() string {
	return sa.Name
}

type Membership struct {
	zkc      *zkc.ZookeeperClient
	dirWatch <-chan zk.Event

	// mu protects aliveNodes and ch
	mu         sync.RWMutex
	aliveNodes sync.Map      // ServerName -> *ServerNode
	ch         *c.Consistent // ServerNode
	self       ServerNode
}

func NewMembership(consistent *c.Consistent, timeout time.Duration, self ServerNode, servers []string) *Membership {
	z := zkc.NewZookeeperClient(timeout, servers)

	m := &Membership{
		ch:   consistent,
		zkc:  z,
		self: self,
	}
	return m
}

func (m *Membership) Init() {
	// Create the chmembership directory if it doesn't exist.
	if exists, _ := m.zkc.Exists(CH_MEMBERSHIP_PATH, false); !exists {
		if _, ok := m.zkc.Create(CH_MEMBERSHIP_PATH, "", 0); !ok {
			log.Panicf("Directory %v not created!\n", CH_MEMBERSHIP_PATH)
		}

		log.Printf("Directory %v created.\n", CH_MEMBERSHIP_PATH)
	}

	// Create ephemeral znode for this server.
	absolutePath := getAbsolutePath(m.self.Name)

	if _, ok := m.zkc.Create(absolutePath, m.self.Addr, zk.FlagEphemeral); !ok {
		log.Panicf("Server membership znode <%v> not created.\n", absolutePath)
	}

	nodes, channel := m.zkc.GetChildren(CH_MEMBERSHIP_PATH, true)
	nodesData, _ := m.zkc.GetDataFromChildren(CH_MEMBERSHIP_PATH, nodes, false)

	m.mu.Lock()
	m.dirWatch = channel

	for i := range nodes {
		// Add to consistent hash ring
		serverName := nodes[i]
		serverAddr := nodesData[i]
		serverNode := &ServerNode{Name: serverName, Addr: serverAddr}
		m.ch.Add(serverNode)
		m.aliveNodes.Store(serverName, serverNode)
	}
	m.mu.Unlock()

	// Monitor membership
	go m.MonitorMembershipDirectory()
}

func (m *Membership) Self() ServerNode {
	return m.self
}

// key -> ServerNode
func (m *Membership) LocateServer(key []byte) *ServerNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ch.LocateKey(key).(*ServerNode)
}

// []key -> []ServerNode
func (m *Membership) GetClosestN(key []byte, count int) []*ServerNode {
	m.mu.RLock()
	members, err := m.ch.GetClosestN(key, count)
	m.mu.RUnlock()
	if err != nil {
		log.Panic(err)
	}

	var serverNodes []*ServerNode
	for _, member := range members {
		serverNodes = append(serverNodes, member.(*ServerNode))
	}

	return serverNodes
}

func (m *Membership) getOldServers() set.Set {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var servers set.Set
	m.aliveNodes.Range(func(k any, _ any) bool {
		servers.Insert(k)
		return true
	})
	return servers
}

func (m *Membership) MonitorMembershipDirectory() {
	for {
		evt := <-m.dirWatch

		nodes, channel := m.zkc.GetChildren(CH_MEMBERSHIP_PATH, true)
		m.dirWatch = channel

		if evt.Type != zk.EventNodeChildrenChanged {
			log.Println("chmembership: MonitorMembershipDirectory: ignoring operation", evt.Type.String())
		} else {
			nodesData, _ := m.zkc.GetDataFromChildren(CH_MEMBERSHIP_PATH, nodes, false)

			oldServers := m.getOldServers()

			var newServers set.Set
			var newServerNodes map[string]*ServerNode
			for i, node := range nodes {
				newServers.Insert(node)
				newServerNodes[node] = &ServerNode{Name: node, Addr: nodesData[i]}
			}

			intersection := oldServers.Intersection(&newServers)
			removedServers := oldServers.Difference(intersection)
			addedServers := newServers.Difference(intersection)

			m.mu.Lock()
			defer m.mu.Unlock()
			removedServers.Do(func(server interface{}) {
				m.aliveNodes.Delete(server)
				m.ch.Remove(server.(string))
			})

			addedServers.Do(func(server interface{}) {
				serverNode := newServerNodes[server.(string)]
				m.aliveNodes.Store(server, serverNode)
				m.ch.Add(serverNode)
			})
		}
	}
}
