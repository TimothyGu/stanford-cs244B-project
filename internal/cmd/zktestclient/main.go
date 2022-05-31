package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

const (
	ZK_SERVER               = "54.219.185.237:2181"
	ZK_TEST_CLIENT_ROOT_DIR = "/zktestclient"
)

var zkServers = flag.String("zkservers", ZK_SERVER, "Comma-separated list of zookeeper server addresses.")

func main() {
	flag.Parse()
	client := zkc.NewZookeeperClient(time.Second, strings.Split(*zkServers, ","))

	if exists, _ := client.Exists(ZK_TEST_CLIENT_ROOT_DIR, false); !exists {
		path, ok := client.Create(ZK_TEST_CLIENT_ROOT_DIR, "", 0)
		fmt.Printf("Path %v, Result Path %v, ok %v.\n", ZK_TEST_CLIENT_ROOT_DIR, path, ok)
	}

	path, ok := client.Create(zkc.GetAbsolutePath(ZK_TEST_CLIENT_ROOT_DIR, "testclient"), "abcd", zk.FlagEphemeral)
	fmt.Printf("Path %v, Result Path %v, ok %v.\n", zkc.GetAbsolutePath(ZK_TEST_CLIENT_ROOT_DIR, "testclient"), path, ok)

	children, watch := client.GetChildren(ZK_TEST_CLIENT_ROOT_DIR, true)
	fmt.Printf("%+v \n", children)

	for _, child := range children {
		fmt.Println("Node path:", child)
		// if children, _ := zkc.GetChildren(child, false); len(children) == 0 {
		// 	continue
		// }

		data, _ := client.GetData(zkc.GetAbsolutePath(ZK_TEST_CLIENT_ROOT_DIR, child), false)
		fmt.Printf("Node: %v, Data: %v \n", child, data)
	}

	for e := range watch {
		fmt.Printf("Watch event: %v.\n", e)
	}
}
