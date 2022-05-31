package hashclustermanager

import (
	"log"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/golang-collections/collections/set"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/types"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
	"google.golang.org/grpc"
)

const (
	ZK_SERVER        = "54.219.185.237:2181"
	NODE_NAME_1      = "HashClusterManager-Test-1"
	NODE_NAME_2      = "HashClusterManager-Test-2"
	TEST_CLIENT_NAME = "HashClusterManager-TestClient"
)

func createTestHCMInstance(joinClusterCallback JoinClusterCallback, exitClusterCallback ExitClusterCallback, nodeName string) *HashClusterManager {
	client := zkc.NewZookeeperClient(time.Second, []string{ZK_SERVER})

	return &HashClusterManager{
		totalClusters:       0,
		preferredClusters:   0,
		maxClusters:         0,
		self:                &types.ServerNode{Name: nodeName, Addr: "localhost:1054"},
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
	hcm := createTestHCMInstance(func(cluster int) {}, func(cluster int) {}, NODE_NAME_1)
	client := zkc.NewZookeeperClient(time.Second, []string{ZK_SERVER})
	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME_1)
	for _, i := range []string{"0", "1", "2"} {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath, i))
		client.Delete(zkc.GetAbsolutePath(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, i), NODE_NAME_1))
	}
	client.Delete(node2ClusterPath)
	client.Delete(SERVICE_PATH)

	hcm.Init()

	// Check if zookeeper membership znodes are correctly created
	if exist, _ := client.Exists(SERVICE_PATH, false); !exist {
		t.Fatalf("%v directory not created!", SERVICE_PATH)
	}

	if exist, _ := client.Exists(NODE_MEMBERSHIP, false); !exist {
		t.Fatalf("%v directory not created!", NODE_MEMBERSHIP)
	}

	children, _ := client.GetChildren(NODE_MEMBERSHIP, false)

	found := false
	var curNode string
	for _, child := range children {
		if strings.Contains(child, getMembershipSequentialZnodePrefix(NODE_NAME_1)) {
			found = true
			curNode = child
			break
		}
	}

	fullPath := zkc.GetAbsolutePath(NODE_MEMBERSHIP, curNode)
	if !found {
		t.Fatalf("membership znode %v not found!", fullPath)
	}

	if value, _ := client.GetData(fullPath, false); value != hcm.self.Addr {
		t.Fatalf("membership znode %v value not correct! expected: %v, actual: %v.", fullPath, hcm.self.Addr, value)
	}

	for _, i := range []string{"0", "1", "2"} {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath, i))
		client.Delete(zkc.GetAbsolutePath(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, i), NODE_NAME_1))
	}
	client.Delete(node2ClusterPath)
	client.Delete(SERVICE_PATH)
}

func preventFromBeingLeader(hcm *HashClusterManager) {
	// Create service directory if it doesn't exist
	hcm.createDirectoryIfNotExist(SERVICE_PATH, "service")

	// Create membership directory if it doesn't exist
	hcm.createDirectoryIfNotExist(NODE_MEMBERSHIP, "membership")

	// Create membership directory if it doesn't exist
	hcm.createDirectoryIfNotExist(NODE2CLUSTER_PATH, "node2cluster")

	// Prevent hcm from being the leader
	if _, ok := hcm.zkClient.Create(
		zkc.GetAbsolutePath(NODE_MEMBERSHIP, getMembershipSequentialZnodePrefix(TEST_CLIENT_NAME)),
		"localhost:1055", zk.FlagEphemeral|zk.FlagSequence); !ok {
		log.Panicf("unable to register membership for %v at %v.\n", TEST_CLIENT_NAME, hcm.self.Addr)
	}
}

func Test_hcm_assignment(t *testing.T) {
	var joinClusterCalls, exitClusterCalls int32
	joinReady := make(chan bool)
	exitReady := make(chan bool)
	joinClusterCallback := func(clusterId int) {
		atomic.AddInt32(&joinClusterCalls, 1)
		joinReady <- true
	}

	exitClusterCallback := func(clusterId int) {
		atomic.AddInt32(&exitClusterCalls, 1)
		exitReady <- true
	}

	hcm := createTestHCMInstance(joinClusterCallback, exitClusterCallback, NODE_NAME_1)
	client := zkc.NewZookeeperClient(time.Second, []string{ZK_SERVER})
	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME_1)
	for _, i := range []string{"0", "1", "2"} {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath, i))
		client.Delete(zkc.GetAbsolutePath(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, i), NODE_NAME_1))
	}
	client.Delete(node2ClusterPath)
	preventFromBeingLeader(hcm)

	go hcm.Init()

	// Create hash cluster 0
	// Register membership
	clusterId := 0
	cluster := "0"
	test_add_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, joinReady, true)
	if joinClusterCalls != 1 {
		t.Fatalf("joinClusterCalls should be 1 instead of %d", joinClusterCalls)
	}

	clusterId = 1
	cluster = "1"
	test_add_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, joinReady, true)
	if joinClusterCalls != 2 {
		t.Fatalf("joinClusterCalls should be 2 instead of %d", joinClusterCalls)
	}

	// Test remove assignment
	test_remove_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, exitReady, true)
	if exitClusterCalls != 1 {
		t.Fatalf("exitClusterCalls should be 1 instead of %d", exitClusterCalls)
	}

	clusterId = 2
	cluster = "2"
	test_add_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, joinReady, true)
	if joinClusterCalls != 3 {
		t.Fatalf("joinClusterCalls should be 3 instead of %d", joinClusterCalls)
	}

	clusterId = 0
	cluster = "0"
	test_remove_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, exitReady, true)
	if exitClusterCalls != 2 {
		t.Fatalf("exitClusterCalls should be 2 instead of %d", exitClusterCalls)
	}

	clusterId = 2
	cluster = "2"
	test_remove_cluster(NODE_NAME_1, hcm, client, t, clusterId, cluster, exitReady, true)
	if exitClusterCalls != 3 {
		t.Fatalf("exitClusterCalls should be 3 instead of %d", exitClusterCalls)
	}

	if hcm.isLeader != false {
		t.Fatal("HCM should not be the leader.")
	}

	// Cleanup
	for _, i := range []string{"0", "1", "2"} {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath, i))
		client.Delete(zkc.GetAbsolutePath(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, i), NODE_NAME_1))
	}
	client.Delete(node2ClusterPath)
}

func Test_hcm_assignment_multinode(t *testing.T) {
	var joinClusterCalls, exitClusterCalls int32
	joinReady := make(chan bool)
	exitReady := make(chan bool)
	joinClusterCallback := func(clusterId int) {
		atomic.AddInt32(&joinClusterCalls, 1)
		log.Println("joinClusterCallback")
		joinReady <- true
	}

	exitClusterCallback := func(clusterId int) {
		atomic.AddInt32(&exitClusterCalls, 1)
		log.Println("exitClusterCallback")
		exitReady <- true
	}
	clusterIds := []int{0, 1, 2, 3}
	clusters := []string{"0", "1", "2", "3"}

	hcm1 := createTestHCMInstance(joinClusterCallback, exitClusterCallback, NODE_NAME_1)
	hcm2 := createTestHCMInstance(joinClusterCallback, exitClusterCallback, NODE_NAME_2)
	client := zkc.NewZookeeperClient(30*time.Second, []string{ZK_SERVER})
	log.Println("Cleaning up...")
	node2ClusterPath1 := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME_1)
	node2ClusterPath2 := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, NODE_NAME_2)
	for _, i := range clusters {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath1, i))
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath2, i))
	}
	client.Delete(node2ClusterPath1)
	client.Delete(node2ClusterPath2)
	preventFromBeingLeader(hcm1)

	go hcm1.Init()
	go hcm2.Init()

	// Create hash cluster 0
	// Register membership
	test_add_cluster(NODE_NAME_1, hcm1, client, t, clusterIds[0], clusters[0], joinReady, true)
	if joinClusterCalls != 1 {
		t.Fatalf("joinClusterCalls should be 1 instead of %d", joinClusterCalls)
	}

	test_add_cluster(NODE_NAME_2, hcm2, client, t, clusterIds[1], clusters[1], joinReady, true)
	if joinClusterCalls != 2 {
		t.Fatalf("joinClusterCalls should be 2 instead of %d", joinClusterCalls)
	}

	test_add_cluster(NODE_NAME_1, hcm1, client, t, clusterIds[2], clusters[2], joinReady, true)
	if joinClusterCalls != 3 {
		t.Fatalf("joinClusterCalls should be 3 instead of %d", joinClusterCalls)
	}

	test_add_cluster(NODE_NAME_2, hcm2, client, t, clusterIds[3], clusters[3], joinReady, true)
	if joinClusterCalls != 4 {
		t.Fatalf("joinClusterCalls should be 4 instead of %d", joinClusterCalls)
	}

	// Now Node 1 should have cluster 0, 2 and Node 2 should have cluster 1, 3
	node1Set, node2Set := set.New(), set.New()
	node1Set.Insert(0)
	node1Set.Insert(2)
	node2Set.Insert(1)
	node2Set.Insert(3)

	time.Sleep(time.Millisecond * 500)
	if hcm1.curAssignments[NODE_NAME_1].Difference(node1Set).Len() != 0 {
		t.Fatalf("Node 1 on node 1 assignments not expected. expected: %v, actual: %v.", node1Set, hcm1.curAssignments[NODE_NAME_1])
	}

	if hcm1.curAssignments[NODE_NAME_2].Difference(node2Set).Len() != 0 {
		t.Fatalf("Node 2 on node 1 assignments not expected. expected: %v, actual: %v.", node2Set, hcm1.curAssignments[NODE_NAME_2])
	}

	if hcm2.curAssignments[NODE_NAME_1].Difference(node1Set).Len() != 0 {
		t.Fatalf("Node 1 on node 2 assignments not expected. expected: %v, actual: %v.", node1Set, hcm1.curAssignments[NODE_NAME_1])
	}

	if hcm2.curAssignments[NODE_NAME_2].Difference(node2Set).Len() != 0 {
		t.Fatalf("Node 2 on node 2 assignments not expected. expected: %v, actual: %v.", node2Set, hcm1.curAssignments[NODE_NAME_2])
	}

	// // Test remove assignment
	test_remove_cluster(NODE_NAME_1, hcm1, client, t, clusterIds[0], clusters[0], exitReady, true)
	if exitClusterCalls != 1 {
		t.Fatalf("exitClusterCalls should be 1 instead of %d", exitClusterCalls)
	}

	test_remove_cluster(NODE_NAME_2, hcm2, client, t, clusterIds[1], clusters[1], exitReady, true)
	if exitClusterCalls != 2 {
		t.Fatalf("exitClusterCalls should be 2 instead of %d", exitClusterCalls)
	}

	test_remove_cluster(NODE_NAME_1, hcm1, client, t, clusterIds[2], clusters[2], exitReady, true)
	if exitClusterCalls != 3 {
		t.Fatalf("exitClusterCalls should be 3 instead of %d", exitClusterCalls)
	}

	test_remove_cluster(NODE_NAME_2, hcm2, client, t, clusterIds[3], clusters[3], exitReady, true)
	if exitClusterCalls != 4 {
		t.Fatalf("exitClusterCalls should be 2 instead of %d", exitClusterCalls)
	}

	time.Sleep(time.Millisecond * 500)
	if hcm1.curAssignments[NODE_NAME_1].Len() != 0 {
		t.Fatalf("Node 1 on node 1 assignments not empty. Actual length=%d.", hcm1.curAssignments[NODE_NAME_1].Len())
	}

	if hcm1.curAssignments[NODE_NAME_2].Len() != 0 {
		t.Fatalf("Node 2 on node 1 assignments not empty. Actual length=%d.", hcm1.curAssignments[NODE_NAME_2].Len())
	}

	if hcm2.curAssignments[NODE_NAME_1].Len() != 0 {
		t.Fatalf("Node 1 on node 2 assignments not empty. Actual length=%d.", hcm2.curAssignments[NODE_NAME_1].Len())
	}

	if hcm2.curAssignments[NODE_NAME_2].Len() != 0 {
		t.Fatalf("Node 2 on node 2 assignments not empty. Actual length=%d.", hcm2.curAssignments[NODE_NAME_2].Len())
	}

	// Cleanup
	log.Println("Cleaning up...")
	for _, i := range clusters {
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath1, i))
		client.Delete(zkc.GetAbsolutePath(node2ClusterPath2, i))
	}
	client.Delete(node2ClusterPath1)
	client.Delete(node2ClusterPath2)
}

func test_add_cluster(
	nodeName string,
	hcm *HashClusterManager,
	client *zkc.ZookeeperClient,
	t *testing.T,
	clusterId int,
	cluster string,
	ready <-chan bool,
	sameNode bool) {
	log.Printf("Test %v's content about node %v", hcm.self.Name, nodeName)
	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, nodeName)
	hcm.createDirectoryIfNotExist(node2ClusterPath, "node2ClusterPath")
	hcm.createDirectoryIfNotExist(zkc.GetAbsolutePath(node2ClusterPath, cluster), "node2ClusterPath")

	_ = <-ready

	if exist, _ := client.Exists(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, nodeName)), false); !exist {
		t.Fatalf("Cluster %v not created! Full path: %v", cluster, zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, nodeName)))
	}

	curAssignments := hcm.curAssignments[nodeName]
	nodes := hcm.cluster2NodeMap[clusterId]

	if sameNode && !curAssignments.Has(clusterId) {
		t.Fatalf("Cluster %v not added to current assignments!", cluster)
	}

	if len(nodes) != 1 || nodes[0] != nodeName {
		if len(nodes) == 1 {
			t.Logf("cluster2NodeMap expected node name: %v, actual: %v", nodeName, nodes[0])
		}
		log.Println("content of cluster2NodeMap", hcm.cluster2NodeMap)
		t.Fatalf("cluster2NodeMap not updated correctly! len(nodes)=%d", len(nodes))
	}
}

func test_remove_cluster(
	nodeName string,
	hcm *HashClusterManager,
	client *zkc.ZookeeperClient,
	t *testing.T,
	clusterId int,
	cluster string,
	ready <-chan bool,
	sameNode bool) {

	node2ClusterPath := zkc.GetAbsolutePath(NODE2CLUSTER_PATH, nodeName)
	client.Delete(zkc.GetAbsolutePath(node2ClusterPath, cluster))

	_ = <-ready

	if exist, _ := client.Exists(zkc.GetAbsolutePath(CLUSTER2NODE_PATH, zkc.GetAbsolutePath(cluster, nodeName)), false); exist {
		t.Fatalf("Cluster %v not deleted!", cluster)
	}

	curAssignments := hcm.curAssignments[nodeName]
	nodes := hcm.cluster2NodeMap[clusterId]

	if sameNode && curAssignments.Has(clusterId) {
		t.Fatalf("Cluster %v not deleted from current assignments!", cluster)
	}

	if len(nodes) != 0 {
		t.Fatalf("cluster2NodeMap not updated correctly! len(nodes)=%d", len(nodes))
	}
}
