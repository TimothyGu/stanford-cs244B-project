package chmembership

import (
	"log"
	"sync"
	"time"

	c "github.com/buraksezer/consistent"
	"github.com/go-zookeeper/zk"
	set "github.com/golang-collections/collections/set"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

const CH_MEMBERSHIP_PATH = "/chmembership"

func getAbsolutePath(relativePath string) string {
	return CH_MEMBERSHIP_PATH + "/" + relativePath
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
	aliveNodes sync.Map // ServerName -> *ServerNode
	mu         sync.RWMutex
	ch         *c.Consistent // ServerNode
	zkc        *zkc.ZookeeperClient
	dirWatch   <-chan zk.Event
}

func NewMembership(consistent *c.Consistent, timeout time.Duration, serverNode ServerNode, servers []string) *Membership {
	z := zkc.NewZookeeperClient(timeout, servers)

	m := &Membership{
		ch:  consistent,
		zkc: z,
	}
	z.Create(serverNode.Name, getAbsolutePath(serverNode.Addr), zk.FlagEphemeral)
	return m
}

func (m *Membership) Init() {
	m.initMembership()
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
	defer m.mu.RUnlock()
	members, err := m.ch.GetClosestN(key, count)
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

func (m *Membership) initMembership() {
	nodes, channel := m.zkc.GetChildren(CH_MEMBERSHIP_PATH, true)
	nodesData, _ := m.zkc.GetDataFromChildren(CH_MEMBERSHIP_PATH, nodes, false)

	m.mu.Lock()
	m.dirWatch = channel

	for i := 0; i < len(nodes); i++ {
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

func (m *Membership) MonitorMembershipDirectory() {
	for {
		evt := <-m.dirWatch

		if evt.Type != zk.EventNodeChildrenChanged {
			log.Println("chmembership: MonitorMembershipDirectory: ignoring operation", evt.Type.String())
		} else {
			nodes, channel := m.zkc.GetChildren(CH_MEMBERSHIP_PATH, true)
			nodesData, _ := m.zkc.GetDataFromChildren(CH_MEMBERSHIP_PATH, nodes, false)

			oldServers := m.getOldServers()

			var newServers set.Set
			var newServerNodes map[string]*ServerNode
			for i, node := range nodes {
				newServers.Insert(node)
				newServerNodes[node] = &ServerNode{Name: node, Addr: nodesData[i]}
			}

			intersection := oldServers.Intersection(&newServers)
			removedServers := intersection.Difference(&oldServers)
			addedServers := intersection.Difference(&newServers)

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

			m.dirWatch = channel
		}
	}
}

func (m *Membership) MonitorNode(name string, path string, nodeChannel <-chan zk.Event) {
	evt := <-nodeChannel
	switch evt.Type {
	case zk.EventNodeDataChanged:
		addr, watchCh := m.zkc.GetData(path, true)
		oldServerNode, ok := m.aliveNodes.Load(name)
		if !ok {
			log.Println("chmembership: MonitorNode: did not find server node name=", name, "at", path)
		} else {
			m.mu.Lock()
			defer m.mu.Unlock()
			oldServerNode.(*ServerNode).Addr = addr

			// TODO: Convert to loop
			go m.MonitorNode(name, path, watchCh)
		}
		break
	case zk.EventNodeDeleted:
		m.mu.Lock()
		defer m.mu.Unlock()
		_, loaded := m.aliveNodes.LoadAndDelete(name)
		if loaded {
			m.ch.Remove(name)
		} else {
			log.Println("chmembership: MonitorNode: did not find server node name=", name, "at", path)
		}
		break
	default:
		log.Println("chmembership: unsupported operation in MonitorNode:", evt.Type.String())
		break
	}
}
