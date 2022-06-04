package chmembership

import (
	"sync"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/go-zookeeper/zk"
	"github.com/golang-collections/collections/set"
	log "github.com/sirupsen/logrus"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

const CH_MEMBERSHIP_PATH = "/chmembership"

func getAbsolutePath(relativePath string) string {
	return zkc.GetAbsolutePath(CH_MEMBERSHIP_PATH, relativePath)
}

/*
	/chmembership/servername(ip:port)
*/

type Membership struct {
	zkc      *zkc.ZookeeperClient
	dirWatch <-chan zk.Event
	self     types.ServerNode

	// mu protects aliveNodes and ch
	mu         sync.RWMutex
	aliveNodes map[string]*types.ServerNode
	ch         *consistent.Consistent // *ServerNode
}

func NewMembership(timeout time.Duration, self types.ServerNode, servers []string) *Membership {
	z := zkc.NewZookeeperClient(timeout, servers)

	m := &Membership{
		zkc:        z,
		self:       self,
		aliveNodes: map[string]*types.ServerNode{},
		ch:         NewConsistentHashing(),
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

	nodes, channel, nodesData, _ := m.zkc.GetChildrenAndData(CH_MEMBERSHIP_PATH, true, false)

	m.mu.Lock()
	for i := range nodes {
		// Add to consistent hash ring
		serverName := nodes[i]
		serverAddr := nodesData[i]
		serverNode := &types.ServerNode{Name: serverName, Addr: serverAddr}
		log.Infof("chmembership: adding server %v @ %v", serverNode.Name, serverNode.Addr)
		m.ch.Add(serverNode)
		m.aliveNodes[serverName] = serverNode
	}
	m.mu.Unlock()

	// Monitor membership
	m.dirWatch = channel
	go m.MonitorMembershipDirectory()
}

func (m *Membership) Self() types.ServerNode {
	return m.self
}

// key -> ServerNode
func (m *Membership) LocateServer(key []byte) *types.ServerNode {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ch.LocateKey(key).(*types.ServerNode)
}

// []key -> []ServerNode
func (m *Membership) GetClosestN(key []byte, count int) []*types.ServerNode {
	m.mu.RLock()
	if n := len(m.ch.GetMembers()); n < count {
		count = n
	}
	members, err := m.ch.GetClosestN(key, count)
	m.mu.RUnlock()
	if err != nil {
		log.Panic(err)
	}

	var serverNodes []*types.ServerNode
	for _, member := range members {
		serverNodes = append(serverNodes, member.(*types.ServerNode))
	}

	return serverNodes
}

func (m *Membership) getOldServers() *set.Set {
	m.mu.RLock()
	defer m.mu.RUnlock()

	servers := set.New()
	for k := range m.aliveNodes {
		servers.Insert(k)
	}
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

			newServers := set.New()
			newServerNodes := map[string]*types.ServerNode{}
			for i, node := range nodes {
				newServers.Insert(node)
				newServerNodes[node] = &types.ServerNode{Name: node, Addr: nodesData[i]}
			}

			intersection := oldServers.Intersection(newServers)
			removedServers := oldServers.Difference(intersection)
			addedServers := newServers.Difference(intersection)

			m.mu.Lock()
			removedServers.Do(func(server interface{}) {
				serverNode, ok := m.aliveNodes[server.(string)]
				if ok {
					log.Infof("chmembership: removing server %v @ %v", serverNode.Name, serverNode.Addr)
				}
				m.ch.Remove(server.(string))
			})
			addedServers.Do(func(server interface{}) {
				serverNode := newServerNodes[server.(string)]
				log.Infof("chmembership: adding server %v @ %v", serverNode.Name, serverNode.Addr)
				m.aliveNodes[server.(string)] = serverNode
				m.ch.Add(serverNode)
			})
			m.mu.Unlock()
		}
	}
}
