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

	path, err := z.zkConn.Create(path, []byte(data), flags, zk.WorldACL(zk.PermAll))
	if err != nil {
		log.Errorln(err)
		ok = false
	}

	return path, ok
}

// Exists returns the children and watch events channel. channel is nil if watch == false
func (z *ZookeeperClient) Exists(path string, watch bool) (exists bool, evtChannel <-chan zk.Event) {
	var err error

	if watch {
		exists, _, evtChannel, err = z.zkConn.ExistsW(path)
	} else {
		exists, _, err = z.zkConn.Exists(path)
	}

	if err != nil {
		panic(err)
	}

	return exists, evtChannel
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

// Delete removes the znode given the path. Here we remove the znode regardless of the version.
func (z *ZookeeperClient) Delete(path string) {
	if err := z.zkConn.Delete(path, -1); err != nil {
		log.Errorln(err)
	}
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
		log.Error(err)
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

func (z *ZookeeperClient) GetChildrenAndData(path string, watchDir bool, watchEachChild bool) (children []string, channel <-chan zk.Event, childrenData []string, childrenWatch []<-chan zk.Event) {
	children, channel = z.GetChildren(path, watchDir)
	if len(children) != 0 {
		childrenData, childrenWatch = z.GetDataFromChildren(path, children, watchEachChild)
	}

	return children, channel, childrenData, childrenWatch
}
