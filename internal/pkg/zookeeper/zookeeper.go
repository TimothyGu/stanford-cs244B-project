package zookeeper

import (
	"time"

	"github.com/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

func GetAbsolutePath(rootPath string, relativePath string) string {
	return rootPath + "/" + relativePath
}

type ZookeeperClient struct {
	zkConn *zk.Conn
}

func NewZookeeperClient(timeout time.Duration, servers []string) *ZookeeperClient {
	c, _, err := zk.Connect(servers, timeout)
	if err != nil {
		panic(err)
	}

	return &ZookeeperClient{zkConn: c}
}

// returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) Create(path string, data string, flags int32) (string, bool) {
	ok := true
	path, err := z.zkConn.Create(path, []byte(data), flags, nil) // No ACL needed

	if err != nil {
		log.Errorln(err)
		ok = false
	}

	return path, ok
}

// GetData returns the children and watch events channel. channel is nil if watch == false
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

// GetChildren returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) GetChildren(path string, watch bool) (children []string, channel <-chan zk.Event) {
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

func (z *ZookeeperClient) GetDataFromChildren(path string, children []string, watch bool) (childrenData []string, childrenWatch []<-chan zk.Event) {
	for _, child := range children {
		data, channel := z.GetData(GetAbsolutePath(path, child), watch)
		childrenData = append(childrenData, data)
		childrenWatch = append(childrenWatch, channel)
	}

	return childrenData, childrenWatch
}
