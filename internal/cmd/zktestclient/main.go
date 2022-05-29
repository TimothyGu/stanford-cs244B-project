package main

import (
	"fmt"
	"time"

	zkc "go.timothygu.me/stanford-cs244b-project/internal/pkg/zookeeper"
)

func main() {
	zkc := zkc.NewZookeeperClient(time.Second)

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
