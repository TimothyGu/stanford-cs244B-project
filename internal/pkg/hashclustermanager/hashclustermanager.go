package hashclustermanager

import (
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

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
	notFilledClusters *set.Set
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
		notFilledClusters:   set.New(),
	}

	return hcm
}

func (hcm *HashClusterManager) createDirectoryIfNotExist(path string, description string) {
	if exist, _ := hcm.zkClient.Exists(path, false); !exist {
		log.Printf("hashclustermanager: %v does not exist yet. Creating...\n", path)

		if _, ok := hcm.zkClient.Create(path, "", 0); !ok {
			log.Panicf("hashclustermanager: unable to create %v directory %v.\n", path, description)
		}
	}
}

func (hcm *HashClusterManager) Init() {
	// Create service directory if it doesn't exist
	hcm.createDirectoryIfNotExist(SERVICE_PATH, "service")

	// Create membership directory if it doesn't exist
	hcm.createDirectoryIfNotExist(NODE_MEMBERSHIP, "membership")

	// Create membership directory if it doesn't exist
	hcm.createDirectoryIfNotExist(NODE2CLUSTER_PATH, "node2cluster")

	// Register membership
	if _, ok := hcm.zkClient.Create(
		zkc.GetAbsolutePath(NODE_MEMBERSHIP, getMembershipSequentialZnodePrefix(hcm.self.Name)),
		hcm.self.Addr, zk.FlagEphemeral|zk.FlagSequence); !ok {
		log.Panicf("hashclustermanager: unable to register membership for %v at %v.\n", hcm.self.Name, hcm.self.Addr)
	}

	go hcm.monitorMembership() // Leader election is handled in monitorMembership
	go hcm.monitorClusterAssignment(hcm.self.Name)
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
	return name + SEQUNTIAL_NODE_SEP
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
	isUpdatingSelf := node == hcm.self.Name
	latestAssgnment := set.New()
	for _, cluster := range clusters {
		latestAssgnment.Insert(getClusterId(cluster))
	}

	// Update local assignments
	hcm.mu.Lock()
	defer hcm.mu.Unlock()

	curAssignments, exists := hcm.curAssignments[node]
	if !exists {
		curAssignments = set.New()
		hcm.curAssignments[node] = curAssignments
	}

	// Find removed assignments and new assignments
	intersection := curAssignments.Intersection(latestAssgnment)
	removedAssignments := curAssignments.Difference(intersection)
	newAssignments := latestAssgnment.Difference(intersection)

	removedAssignments.Do(func(clusterId any) {
		curAssignments.Remove(clusterId)
		hcm.cluster2NodeMap[clusterId.(int)] = removeItem(hcm.cluster2NodeMap[clusterId.(int)], node)

		if isUpdatingSelf {
			// Exit Cluster Callback
			go hcm.exitCluster(clusterId.(int), strconv.Itoa(clusterId.(int)))
		}
	})

	newAssignments.Do(func(clusterId any) {
		curAssignments.Insert(clusterId)
		hcm.cluster2NodeMap[clusterId.(int)] = append(hcm.cluster2NodeMap[clusterId.(int)], node)

		if isUpdatingSelf {
			// Join Cluster Callback
			go hcm.joinCluster(clusterId.(int), strconv.Itoa(clusterId.(int)))
		}
	})
}

func (hcm *HashClusterManager) monitorClusterAssignment(node string) {
	evt := zk.Event{Type: zk.EventNodeChildrenChanged}

	for {
		// hcm.mu.RLock()
		// if _, exists := hcm.curAssignments[node]; !exists {
		// 	hcm.mu.RUnlock()
		// 	break
		// }
		// hcm.mu.RUnlock()

		clusters, _ := hcm.zkClient.GetChildren(zkc.GetAbsolutePath(NODE2CLUSTER_PATH, node), true)

		// We only care if the children are changed (assignment changed).
		// We ignore other types of updates and do not expect them to happen.
		if evt.Type == zk.EventNodeChildrenChanged {
			hcm.updateCurrentClusterAssignment(clusters, node)
		}

		// Unfortunately, it seems the ZK library will deadlock or starve when using the watch.
		// This renders our design impossible to implement.
		time.Sleep(time.Millisecond * 500)
		// evt = <-watch
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
		node, _ := rec.Key.(string), rec.Val.(int)
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
	if len(sequentialNodes) == 0 {
		return
	}

	newNodeSet := set.New()
	oldNodeSet := set.New()

	// seqNum int -> node string
	seqNum2Node := sortedmap.New(len(sequentialNodes), func(i, j any) bool {
		return i.(int) < j.(int)
	})

	for _, sequentialNode := range sequentialNodes {
		data := strings.Split(sequentialNode, SEQUNTIAL_NODE_SEP)
		if len(data) != 2 {
			log.Panicf("hashclustermanager: updateMembership: %v is not in the right format! Should be <node name>_<seq #>.\n", sequentialNode)
		}

		seqNum2Node.Insert(data[0], getClusterId(data[1]))
		newNodeSet.Insert(data[0])
	}

	go hcm.leaderElection(seqNum2Node)

	hcm.mu.Lock()
	for server := range hcm.curAssignments {
		oldNodeSet.Insert(server)
	}

	// Find removed nodes and new nodes
	intersection := oldNodeSet.Intersection(newNodeSet)
	removedNodes := oldNodeSet.Difference(intersection)
	newNodes := newNodeSet.Difference(intersection)

	// Update local memberships

	// We don't need to manually delete the dead znode under CLUSTER2NODE_PATH because
	// 1. they're also ephemeral and
	// 2. we received a notification about an ephemeral znode getting deleted,
	// which means the server / node died.
	removedNodes.Do(func(node any) {
		clusters := hcm.curAssignments[node.(string)]
		clusters.Do(func(clusterId any) {
			hcm.cluster2NodeMap[clusterId.(int)] = removeItem(hcm.cluster2NodeMap[clusterId.(int)], node.(string))
		})
		delete(hcm.curAssignments, node.(string))
	})
	hcm.mu.Unlock()

	for i, node := range seqNum2Node.Keys() {
		if newNodes.Has(node) {
			if node != hcm.self.Name {
				// newNodes don't contain this node.
				go hcm.monitorClusterAssignment(node.(string))

				// This function holds a lock

				nodeAddr := nodeAddrs[i]
				hcm.mu.Lock()

				// Create a new gRpc connection to the new node.
				hcm.gRpcConns[node.(string)] = getGrpcConn(nodeAddr)
				hcm.mu.Unlock()
			}
		}
	}

	if _, exists := hcm.gRpcConns[hcm.self.Name]; !exists {
		hcm.gRpcConns[hcm.self.Name] = getGrpcConn(hcm.self.Addr)
	}
}

func (hcm *HashClusterManager) monitorMembership() {
	evt := zk.Event{Type: zk.EventNodeChildrenChanged}

	for {
		nodes, _, nodeAddrs, _ := hcm.zkClient.GetChildrenAndData(NODE_MEMBERSHIP, true, false)

		_ = nodes
		_ = nodeAddrs

		// We only care if the children are changed (membership changed).
		// We ignore other types of updates and do not expect them to happen.
		if evt.Type == zk.EventNodeChildrenChanged {
			hcm.updateMembership(nodes, nodeAddrs)
		}

		time.Sleep(time.Millisecond * 500)
		// evt = <-watch
	}
}

func (hcm *HashClusterManager) allocateClusters(clusterId int) {
	// TODO
}

func (hcm *HashClusterManager) joinCluster(clusterId int, cluster string) {
	if exists, _ := hcm.zkClient.Exists(CLUSTER2NODE_PATH, false); !exists {
		log.Panicf("hashclustermanager: %v does not exist.", CLUSTER2NODE_PATH)
	}

	if exists, _ := hcm.zkClient.Exists(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster), false); !exists {
		if _, ok := hcm.zkClient.Create(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, cluster), "", 0); !ok {
			log.Panicf("hashclustermanager: create cluster directory %v \n", cluster)
		}
	}

	if _, ok := hcm.zkClient.Create(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, hcm.self.Name)), "", zk.FlagEphemeral); !ok {
		log.Panicf("hashclustermanager: create ephemeral znode %v failed for cluster %v \n", hcm.self.Name, cluster)
	}
	hcm.joinClusterCallback(clusterId)
}

func (hcm *HashClusterManager) exitCluster(clusterId int, cluster string) {
	hcm.zkClient.Delete(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, hcm.self.Name)))
	hcm.exitClusterCallback(clusterId)
}
