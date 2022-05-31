package hashclustermanager

import (
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-collections/collections/set"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
	"google.golang.org/grpc"
)

const (
	ZK_SERVER = "54.219.185.237:2181"
	NODE_NAME = "HashClusterManager-Test"
)

func createTestHCMInstance(joinClusterCallback JoinClusterCallback, exitClusterCallback ExitClusterCallback) *HashClusterManager {
	client := zkc.NewZookeeperClient(time.Second, []string{ZK_SERVER})

	return &HashClusterManager{
		totalClusters:       0,
		preferredClusters:   0,
		maxClusters:         0,
		self:                &types.ServerNode{Name: NODE_NAME, Addr: "localhost:1054"},
		zkClient:            client,
		joinClusterCallback: joinClusterCallback,
		exitClusterCallback: exitClusterCallback,
		gRpcConns:           map[string]*grpc.ClientConn{},
		cluster2NodeMap:     map[int][]string{},
		curAssignments:      map[string]*set.Set{},
		isLeader:            false,
		clusterSize:         map[int]int{},
		notFilledClusters:   set.New(),
	}
}

func Test_hcm_init(t *testing.T) {
	hcm := createTestHCMInstance(func(cluster int) {}, func(cluster int) {})
	hcm.Init()

	// Check if zookeeper membership znodes are correctly created
	if exist, _ := hcm.zkClient.Exists(SERVICE_PATH, false); !exist {
		t.Fatalf("%v directory not created!", SERVICE_PATH)
	}

	if exist, _ := hcm.zkClient.Exists(NODE_MEMBERSHIP, false); !exist {
		t.Fatalf("%v directory not created!", NODE_MEMBERSHIP)
	}

	children, _ := hcm.zkClient.GetChildren(NODE_MEMBERSHIP, false)

	found := false
	var curNode string
	for _, child := range children {
		if strings.Contains(child, getMembershipSequentialZnodePrefix(NODE_NAME)) {
			found = true
			curNode = child
			break
		}
	}

	fullPath := zkc.GetAbsolutePath(NODE_MEMBERSHIP, curNode)
	if !found {
		t.Fatalf("membership znode %v not found!", fullPath)
	}

	if value, _ := hcm.zkClient.GetData(fullPath, false); value != hcm.self.Addr {
		t.Fatalf("membership znode %v value not correct! expected: %v, actual: %v.", fullPath, hcm.self.Addr, value)
	}
}

func Test_hcm_assignment(t *testing.T) {
	var joinClusterCalls, exitClusterCalls int32
	joinCounter, exitCounter := sync.Map{}, sync.Map{}
	joinClusterCallback := func(clusterId int) {
		atomic.AddInt32(&joinClusterCalls, 1)
		count, _ := joinCounter.LoadOrStore(clusterId, 0)
		joinCounter.Store(clusterId, count.(int)+1)
	}

	exitClusterCallback := func(clusterId int) {
		atomic.AddInt32(&exitClusterCalls, 1)
		count, _ := exitCounter.LoadOrStore(clusterId, 0)
		exitCounter.Store(clusterId, count.(int)+1)
	}

	hcm := createTestHCMInstance(joinClusterCallback, exitClusterCallback)
	hcm.Init()

	// Create hash cluster 0
	// Register membership
	clusterId := 0
	cluster := "0"
	test_add_cluster(hcm, t, clusterId, cluster, &joinCounter, &exitCounter)

	clusterId = 1
	cluster = "1"
	test_add_cluster(hcm, t, clusterId, cluster, &joinCounter, &exitCounter)

	// Test remove assignment
	test_remove_cluster(hcm, t, clusterId, cluster, &exitCounter)

	clusterId = 2
	cluster = "2"
	test_add_cluster(hcm, t, clusterId, cluster, &joinCounter, &exitCounter)

	clusterId = 0
	cluster = "0"
	test_remove_cluster(hcm, t, clusterId, cluster, &exitCounter)

	// Should be no op
	exitCounter.Store(clusterId, 1)
	test_remove_cluster(hcm, t, clusterId, cluster, &exitCounter)

	clusterId = 2
	cluster = "2"
	test_remove_cluster(hcm, t, clusterId, cluster, &exitCounter)

}

func test_add_cluster(
	hcm *HashClusterManager,
	t *testing.T,
	clusterId int,
	cluster string,
	joinCounter *sync.Map,
	exitCounter *sync.Map) {

	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME)
	hcm.createDirectoryIfNotExist(node2ClusterPath, "node2ClusterPath")
	hcm.createDirectoryIfNotExist(zkc.GetAbsolutePath(node2ClusterPath, cluster), "node2ClusterPath")

	// Give it time to finish the job
	time.Sleep(time.Millisecond * 200)
	if exist, _ := hcm.zkClient.Exists(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, NODE_NAME)), false); !exist {
		t.Fatalf("Cluster %v not created!", cluster)
	}

	curAssignments := hcm.curAssignments[NODE_NAME]
	nodes := hcm.cluster2NodeMap[clusterId]

	if !curAssignments.Has(clusterId) {
		t.Fatalf("Cluster %v not added to current assignments!", cluster)
	}

	if count, _ := joinCounter.Load(clusterId); count != 1 {
		t.Fatalf("join counter not updated correctly! count=%d, expected: 1", count.(int))
	}

	if count, ok := exitCounter.Load(clusterId); ok && count != 0 {
		t.Fatalf("exit counter not updated correctly! count=%d, expected: 0", count.(int))
	}

	if len(nodes) != 1 || nodes[0] != NODE_NAME {
		if len(nodes) == 1 {
			t.Logf("cluster2NodeMap expected node name: %v, actual: %v", NODE_NAME, nodes[0])
		}
		log.Println("content of cluster2NodeMap", hcm.cluster2NodeMap)
		t.Fatalf("cluster2NodeMap not updated correctly! len(nodes)=%d", len(nodes))
	}

	joinCounter.Store(clusterId, 0)
	exitCounter.Store(clusterId, 0)
}

func test_remove_cluster(
	hcm *HashClusterManager,
	t *testing.T,
	clusterId int,
	cluster string,
	exitCounter *sync.Map) {

	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME)
	hcm.zkClient.Delete(zkc.GetAbsolutePath(node2ClusterPath, cluster))

	// Give it time to finish the job
	time.Sleep(time.Millisecond * 200)

	if count, ok := exitCounter.Load(clusterId); !ok || count != 1 {
		t.Fatalf("exit counter not updated correctly for cluster %d!", clusterId)
	}

	if exist, _ := hcm.zkClient.Exists(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, NODE_NAME)), false); exist {
		t.Fatalf("Cluster %v not deleted!", cluster)
	}

	curAssignments := hcm.curAssignments[NODE_NAME]
	nodes := hcm.cluster2NodeMap[clusterId]

	if curAssignments.Has(clusterId) {
		t.Fatalf("Cluster %v not deleted from current assignments!", cluster)
	}

	if len(nodes) != 0 {
		t.Fatalf("cluster2NodeMap not updated correctly! len(nodes)=%d", len(nodes))
	}

	exitCounter.Store(clusterId, 0)
}
