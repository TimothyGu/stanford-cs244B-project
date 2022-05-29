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

func NewMembership(consistent *c.Consistent, timeout time.Duration, serverNode ServerNode) *Membership {
	z := zkc.NewZookeeperClient(timeout)

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
func (m *Membership) LocateServer(key []byte) c.Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ch.LocateKey(key)
}

// []key -> []ServerNode
func (m *Membership) GetClosestN(key []byte, count int) []c.Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	members, err := m.ch.GetClosestN(key, count)
	if err != nil {
		log.Panic(err)
	}

	return members
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
	nodesData, nodeChannels := m.zkc.GetDataFromChildren(CH_MEMBERSHIP_PATH, nodes, true)

	// oldServers := m.getOldServers()

	// var newServers set.Set
	// for _, node := range nodes {
	// 	newServers.Insert(node)
	// }

	// intersection := oldServers.Intersection(&newServers)
	// removedServers := intersection.Difference(&oldServers)
	// addedServers := intersection.Difference(&newServers)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirWatch = channel

	for i := 0; i < len(nodes); i++ {
		// Add to consistent hash ring
		serverName := nodes[i]
		serverAddr := nodesData[i]
		serverNode := &ServerNode{Name: serverName, Addr: serverAddr}
		m.ch.Add(serverNode)
		m.aliveNodes.Store(serverName, serverNode)

		// Monitor membership
		go m.MonitorNode(serverName, m.getPath(nodes[i]), nodeChannels[i])
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
