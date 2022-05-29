package main

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	ZK_SERVER = "54.219.185.237:2181"
)

func main() {
	c, _, err := zk.Connect([]string{ZK_SERVER}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	children, stat, _, err := c.ChildrenW("/")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%+v %+v\n", children, stat)
}
