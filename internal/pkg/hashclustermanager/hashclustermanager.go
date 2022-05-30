package hashclustermanager

import (
	"strconv"
	"strings"
	"sync"

	"github.com/go-zookeeper/zk"
	"github.com/golang-collections/collections/set"
	log "github.com/sirupsen/logrus"
	"github.com/umpc/go-sortedmap"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
	"google.golang.org/grpc"
)

type HashClusterManager struct {
	// Configs
	totalClusters     int // Total # clusters
	preferredClusters int // Preferred # clusters for a single node
	maxClusters       int // Max # of clusters for a sinle node

	self     *types.ServerNode
	zkClient *zkc.ZookeeperClient

	// RW Lock protected region
	mu sync.RWMutex

	//
	// Common data for all nodes
	//

	// Node Name -> Grpc Client Connection. Updated by updateMembership.
	gRpcConns map[string]*grpc.ClientConn

	// Cluster ID -> set of Node Names. Updated by updateMembership.
	cluster2NodeMap map[int]*set.Set

	// Node Name -> set of clusters ID (int) assigned.
	// Updated by updateClusterAssignment and updateMembership.
	curAssignments map[string]*set.Set

	// Whether this node is the leader
	isLeader bool

	//
	// Exclusive data for the leader
	//

	// Cluster ID -> # Nodes in the cluster
	clusterSize map[int]int

	// Set of clusters that are below the preferredClustrs parameter
	notFilledClusters set.Set
}

func NewHashClusterManager(
	totalClusters int,
	preferredClusters int,
	maxClusters int,
	self *types.ServerNode,
	zkClient *zkc.ZookeeperClient) *HashClusterManager {

	hcm := &HashClusterManager{
		totalClusters:     totalClusters,
		preferredClusters: preferredClusters,
		maxClusters:       maxClusters,
		self:              self,
		zkClient:          zkClient,
		gRpcConns:         map[string]*grpc.ClientConn{},
		cluster2NodeMap:   map[int]*set.Set{},
		curAssignments:    map[string]*set.Set{},
		isLeader:          false,
		clusterSize:       map[int]int{},
		notFilledClusters: set.Set{},
	}

	return hcm
}

func getMembershipSequentialZnodePrefix(name string) string {
	return name + "_"
}

func (hcm *HashClusterManager) Init() {
	// Register membership
	if _, ok := hcm.zkClient.Create(
		zkc.GetAbsolutePath(NODE_MEMBERSHIP, getMembershipSequentialZnodePrefix(hcm.self.Name)),
		hcm.self.Addr, zk.FlagEphemeral|zk.FlagSequence); !ok {
		log.Panicf("hashclustermanager: unable to register membership for %v at %v.\n", hcm.self.Name, hcm.self.Addr)
	}

	go hcm.monitorMembership() // Leader election is handled in monitorMembership
	go hcm.monitorClusterAssignment()
}

func getClusterId(cluster string) int {
	id, err := strconv.Atoi(cluster)
	if err != nil {
		log.Panicf("hashclustermanager: unable to convert %v to int.\n", cluster)
	}
	return id
}

// updateClusterAssignment updates curAssignments
func (hcm *HashClusterManager) updateClusterAssignment(clusters []string, node string) {
	var newAssignment set.Set
	for _, cluster := range clusters {
		newAssignment.Insert(cluster)
	}

	// Update local assignments
	hcm.mu.Lock()
	defer hcm.mu.Unlock()

	curAssignments := hcm.curAssignments[node]

	// Find removed assignments and new assignments
	intersection := curAssignments.Intersection(&newAssignment)
	removedAssignments := curAssignments.Difference(intersection)
	newAssignments := newAssignment.Difference(intersection)

	removedAssignments.Do(func(cluster interface{}) {
		curAssignments.Remove(getClusterId(cluster.(string)))
	})

	newAssignments.Do(func(cluster interface{}) {
		curAssignments.Insert(getClusterId(cluster.(string)))
	})

	// Update assignments on ZK
	go func() {
		removedAssignments.Do(func(cluster interface{}) {
			hcm.zkClient.Delete(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster.(string)))
		})

		newAssignments.Do(func(cluster interface{}) {
			if _, ok := hcm.zkClient.Create(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster.(string)), "", zk.FlagEphemeral); !ok {
				log.Panicln("hashclustermanager: create ephemeral znode failed for cluster", cluster)
			}
		})
	}()
}

func (hcm *HashClusterManager) monitorClusterAssignment() {
	evt := zk.Event{Type: zk.EventNodeChildrenChanged}

	for {
		clusters, watch := hcm.zkClient.GetChildren(zkc.GetAbsolutePath(NODE2CLUSTER_PATH, hcm.self.Name), true)

		// We only care if the children are changed (assignment changed).
		// We ignore other types of updates and do not expect them to happen.
		if evt.Type == zk.EventNodeChildrenChanged {
			hcm.updateClusterAssignment(clusters, hcm.self.Name)
		}

		evt = <-watch
	}
}

func getGrpcConn(addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Panicf("hashclustermanager: failed to dial gRPC: %v.\n", conn)
	}
	return conn
}

func (hcm *HashClusterManager) leaderElection(seqNum2Node *sortedmap.SortedMap) {
	var nodeBeforeCurrentNode string
	smallest := true

	seqNum2Node.IterFunc(false, func(rec sortedmap.Record) bool {
		_, node := rec.Key.(int), rec.Val.(string)
		if smallest {
			if node == hcm.self.Name {
				return false // break
			} else {
				smallest = false
			}
		} else if node == hcm.self.Name {
			return false
		}

		nodeBeforeCurrentNode = node
		return true
	})

	if smallest {
		// I'm the leader now
		// Start leader procedure
		hcm.mu.Lock()
		defer hcm.mu.Unlock()

		hcm.isLeader = true
	} else {
		// Watch m_j where j is the larges number that is smaller than i
		hcm.isLeader = false
		hcm.zkClient.Exists(zkc.GetAbsolutePath(NODE_MEMBERSHIP, nodeBeforeCurrentNode), true)
	}
}

func (hcm *HashClusterManager) updateMembership(sequentialNodes []string, nodeAddrs []string) {
	var newNodeSet, oldNodeSet set.Set

	// seqNum int -> node string
	seqNum2Node := sortedmap.New(len(sequentialNodes), func(i, j any) bool {
		return i.(int) < j.(int)
	})

	for _, sequentialNode := range sequentialNodes {
		data := strings.Split(sequentialNode, SEQUNTIAL_NODE_SEP)
		if len(data) != 2 {
			log.Panicf("hashclustermanager: %v is not in the right format! Should be <node name>_<seq #>.\n", sequentialNode)
		}

		newNodeSet.Insert(data[0])
		seqNum2Node.Insert(getClusterId(data[1]), data[0])
	}

	for server, _ := range hcm.gRpcConns {
		oldNodeSet.Insert(server)
	}

	// Find removed nodes and new nodes
	intersection := oldNodeSet.Intersection(&newNodeSet)
	removedNodes := oldNodeSet.Difference(intersection)
	newNodes := newNodeSet.Difference(intersection)

	// Update local memberships
	hcm.mu.Lock()

	// We don't need to manually delete the dead znode under CLUSTER2NODE_PATH because
	// 1. they're also ephemeral and
	// 2. we received a notification about an ephemeral znode getting deleted,
	// which means the server / node died.
	removedNodes.Do(func(node interface{}) {
		clusters := hcm.curAssignments[node.(string)]
		clusters.Do(func(cluster interface{}) {
			nodes := hcm.cluster2NodeMap[getClusterId(cluster.(string))]
			nodes.Remove(node)
		})
		delete(hcm.curAssignments, node.(string))
	})
	hcm.mu.Unlock()

	for i, node := range sequentialNodes {
		if newNodes.Has(node) {
			clusters, _ := hcm.zkClient.GetChildren(zkc.GetAbsolutePath(NODE2CLUSTER_PATH, node), false)

			// This function holds a lock
			hcm.updateClusterAssignment(clusters, node)

			nodeAddr := nodeAddrs[i]

			hcm.mu.Lock()
			curAssignments := hcm.curAssignments[node]
			curAssignments.Do(func(cluster interface{}) {
				hcm.cluster2NodeMap[getClusterId(cluster.(string))].Insert(node)
			})

			// Create a new gRpc connection to the new node.
			hcm.gRpcConns[node] = getGrpcConn(nodeAddr)
			hcm.mu.Unlock()
		}
	}

	go hcm.leaderElection(seqNum2Node)
}

func (hcm *HashClusterManager) monitorMembership() {
	evt := zk.Event{Type: zk.EventNodeChildrenChanged}

	for {
		nodes, watch, nodeAddrs, _ := hcm.zkClient.GetChildrenAndData(
			zkc.GetAbsolutePath(NODE2CLUSTER_PATH, hcm.self.Name), true, false)

		_ = nodes
		_ = nodeAddrs

		// We only care if the children are changed (membership changed).
		// We ignore other types of updates and do not expect them to happen.
		if evt.Type == zk.EventNodeChildrenChanged {
			hcm.updateMembership(nodes, nodeAddrs)
		}

		evt = <-watch
	}
}
