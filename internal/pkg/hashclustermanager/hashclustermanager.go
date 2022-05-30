package hashclustermanager

import (
	"math/rand"
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

type JoinClusterCallback func(cluster int)
type ExitClusterCallback func(cluster int)

type HashClusterManager struct {
	// Configs
	totalClusters     int // Total # clusters
	preferredClusters int // Preferred # clusters for a single node
	maxClusters       int // Max # of clusters for a sinle node

	self     *types.ServerNode
	zkClient *zkc.ZookeeperClient

	joinClusterCallback JoinClusterCallback
	exitClusterCallback ExitClusterCallback

	// RW Lock protected region
	mu sync.RWMutex

	//
	// Common data for all nodes
	//

	// Node Name -> Grpc Client Connection. Updated by updateMembership.
	gRpcConns map[string]*grpc.ClientConn

	// Cluster ID -> list of Node Names. Updated by updateMembership.
	cluster2NodeMap map[int][]string

	// Node Name -> set of clusters ID (int) assigned.
	// Updated by updateCurrentClusterAssignment and updateMembership.
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
	zkClient *zkc.ZookeeperClient,
	joinClusterCallback JoinClusterCallback,
	exitClusterCallback ExitClusterCallback) *HashClusterManager {

	hcm := &HashClusterManager{
		totalClusters:       totalClusters,
		preferredClusters:   preferredClusters,
		maxClusters:         maxClusters,
		self:                self,
		zkClient:            zkClient,
		joinClusterCallback: joinClusterCallback,
		exitClusterCallback: exitClusterCallback,
		gRpcConns:           map[string]*grpc.ClientConn{},
		cluster2NodeMap:     map[int][]string{},
		curAssignments:      map[string]*set.Set{},
		isLeader:            false,
		clusterSize:         map[int]int{},
		notFilledClusters:   set.Set{},
	}

	return hcm
}

func (hcm *HashClusterManager) Init() {
	// Create membership directory if it doesn't exist
	if exist, _ := hcm.zkClient.Exists(NODE_MEMBERSHIP, false); !exist {
		log.Printf("hashclustermanager: %v does not exist yet. Creating...\n", NODE_MEMBERSHIP)

		if _, ok := hcm.zkClient.Create(NODE_MEMBERSHIP, "", 0); !ok {
			log.Panicf("hashclustermanager: unable to create membership directory %v.\n", NODE_MEMBERSHIP)
		}
	}

	// Register membership
	if _, ok := hcm.zkClient.Create(
		zkc.GetAbsolutePath(NODE_MEMBERSHIP, getMembershipSequentialZnodePrefix(hcm.self.Name)),
		hcm.self.Addr, zk.FlagEphemeral|zk.FlagSequence); !ok {
		log.Panicf("hashclustermanager: unable to register membership for %v at %v.\n", hcm.self.Name, hcm.self.Addr)
	}

	go hcm.monitorMembership() // Leader election is handled in monitorMembership
	go hcm.monitorClusterAssignment()
}

// GetGrpcConnForPartition returns a server randomly chosen from the server pool. ok indictes whether the connection is found.
// Partition ID is the same as the cluster ID.
func (hcm *HashClusterManager) GetGrpcConnForPartition(partitionId int) (conn *grpc.ClientConn, ok bool) {
	hcm.mu.RLock()
	defer hcm.mu.RUnlock()
	nodes, ok := hcm.cluster2NodeMap[partitionId]
	if !ok {
		return nil, false
	}
	node := nodes[rand.Intn(len(nodes))]
	return hcm.gRpcConns[node], true
}

// Private functions

func remove(s []string, i int) []string {
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

func removeItem(slice []string, item string) []string {
	itemIdx := -1
	for idx, s := range slice {
		if s == item {
			itemIdx = idx
		}
	}

	if itemIdx == -1 {
		return slice
	}

	return remove(slice, itemIdx)
}

func getMembershipSequentialZnodePrefix(name string) string {
	return name + "_"
}

func getClusterId(cluster string) int {
	id, err := strconv.Atoi(cluster)
	if err != nil {
		log.Panicf("hashclustermanager: unable to convert %v to int.\n", cluster)
	}
	return id
}

// updateCurrentClusterAssignment updates curAssignments
func (hcm *HashClusterManager) updateCurrentClusterAssignment(clusters []string, node string) {
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

	removedAssignments.Do(func(cluster any) {
		clusterId := getClusterId(cluster.(string))
		curAssignments.Remove(clusterId)

		// Exit Cluster Callback
		go hcm.exitCluster(clusterId, cluster.(string))
	})

	newAssignments.Do(func(cluster any) {
		clusterId := getClusterId(cluster.(string))
		curAssignments.Insert(clusterId)

		// Join Cluster Callback
		go hcm.joinCluster(clusterId, cluster.(string))
	})
}

func (hcm *HashClusterManager) monitorClusterAssignment() {
	evt := zk.Event{Type: zk.EventNodeChildrenChanged}

	for {
		clusters, watch := hcm.zkClient.GetChildren(zkc.GetAbsolutePath(NODE2CLUSTER_PATH, hcm.self.Name), true)

		// We only care if the children are changed (assignment changed).
		// We ignore other types of updates and do not expect them to happen.
		if evt.Type == zk.EventNodeChildrenChanged {
			hcm.updateCurrentClusterAssignment(clusters, hcm.self.Name)
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

	if !smallest {
		// Watch m_j where j is the larges number that is smaller than i
		hcm.zkClient.Exists(zkc.GetAbsolutePath(NODE_MEMBERSHIP, nodeBeforeCurrentNode), true)
	} else {
		// I'm the leader now
		// Start leader procedure
		// TODO: leader election procedure

		hcm.mu.Lock()
		defer hcm.mu.Unlock()
		hcm.isLeader = true
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

	// We don't need to manually delete the dead znode under CLUSTER2NODE_PATH because
	// 1. they're also ephemeral and
	// 2. we received a notification about an ephemeral znode getting deleted,
	// which means the server / node died.
	removedNodes.Do(func(node any) {
		hcm.mu.Lock()
		defer hcm.mu.Unlock()
		clusters := hcm.curAssignments[node.(string)]
		clusters.Do(func(cluster any) {
			nodes := hcm.cluster2NodeMap[getClusterId(cluster.(string))]
			hcm.cluster2NodeMap[getClusterId(cluster.(string))] = removeItem(nodes, node.(string))
		})
		delete(hcm.curAssignments, node.(string))
	})

	for i, node := range sequentialNodes {
		if newNodes.Has(node) {
			clusters, _ := hcm.zkClient.GetChildren(zkc.GetAbsolutePath(NODE2CLUSTER_PATH, node), false)

			// This function holds a lock
			hcm.updateCurrentClusterAssignment(clusters, node)

			nodeAddr := nodeAddrs[i]

			hcm.mu.Lock()
			curAssignments := hcm.curAssignments[node]
			curAssignments.Do(func(cluster any) {
				nodes := hcm.cluster2NodeMap[getClusterId(cluster.(string))]
				hcm.cluster2NodeMap[getClusterId(cluster.(string))] = append(nodes, node)
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

func (hcm *HashClusterManager) allocateClusters(clusterId int) {
	// TODO
}

func (hcm *HashClusterManager) joinCluster(clusterId int, cluster string) {
	hcm.joinClusterCallback(clusterId)
	if _, ok := hcm.zkClient.Create(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster), "", 0); !ok {
		log.Panicln("hashclustermanager: create znode failed for cluster", cluster)
	}

	if _, ok := hcm.zkClient.Create(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, hcm.self.Name)), "", zk.FlagEphemeral); !ok {
		log.Panicf("hashclustermanager: create ephemeral znode %v failed for cluster %v \n", hcm.self.Name, cluster)
	}
}

func (hcm *HashClusterManager) exitCluster(clusterId int, cluster string) {
	hcm.exitClusterCallback(clusterId)
	hcm.zkClient.Delete(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster))
}
