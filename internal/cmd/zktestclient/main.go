package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

const ZK_SERVER = "54.219.185.237:2181"

var zkServers = flag.String("zkservers", ZK_SERVER, "Comma-separated list of zookeeper server addresses.")

func main() {
	zkc := zkc.NewZookeeperClient(time.Second, strings.Split(*zkServers, ","))

	children, watch := zkc.GetChildren("/", true)
	fmt.Printf("%+v \n", children)

	for _, child := range children {
		fmt.Println("Node path:", child)
		// if children, _ := zkc.GetChildren(child, false); len(children) == 0 {
		// 	continue
		// }

		data, _ := zkc.GetData("/"+child, false)
		fmt.Printf("Node: %v, Data: %v \n", child, data)
	}

	for e := range watch {
		fmt.Printf("Watch event: %v.\n", e)
	}
}
