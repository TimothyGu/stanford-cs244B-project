package zookeeper

import (
	"log"
	"time"

	"github.com/go-zookeeper/zk"
)

const ZK_SERVER = "54.219.185.237:2181"

type ZookeeperClient struct {
	zkConn *zk.Conn
}

func NewZookeeperClient(timeout time.Duration) *ZookeeperClient {
	c, _, err := zk.Connect([]string{ZK_SERVER}, timeout)
	if err != nil {
		panic(err)
	}

	return &ZookeeperClient{zkConn: c}
}

// returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) Create(path string, data string, flags int32) (string, bool) {
	ok := true
	path, err := z.zkConn.Create(path, []byte(data), flags, []zk.ACL{}) // No ACL needed

	if err != nil {
		log.Println(err)
		ok = false
	}

	return path, ok
}

// returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) GetData(path string, watch bool) (string, <-chan zk.Event) {
	var data []byte
	var channel <-chan zk.Event
	var err error

	if watch {
		data, _, channel, err = z.zkConn.GetW(path)
	} else {
		data, _, err = z.zkConn.Get(path)
	}

	if err != nil {
		panic(err)
	}

	return string(data), channel
}

// returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) GetChildren(path string, watch bool) ([]string, <-chan zk.Event) {
	var children []string
	var channel <-chan zk.Event
	var err error

	if watch {
		children, _, channel, err = z.zkConn.ChildrenW(path)
	} else {
		children, _, err = z.zkConn.Children(path)
	}

	if err != nil {
		panic(err)
	}

	return children, channel
}

func (z *ZookeeperClient) GetDataFromChildren(path string, children []string, watch bool) ([]string, []<-chan zk.Event) {
	var childrenData []string
	var childrenWatch []<-chan zk.Event
	for _, child := range children {
		data, channel := z.GetData(path+"/"+child, watch)
		childrenData = append(childrenData, data)
		childrenWatch = append(childrenWatch, channel)
	}

	return childrenData, childrenWatch
}
