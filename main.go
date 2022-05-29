package main

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"go.timothygu.me/stanford-cs244b-project/internal/pkg/chmembership"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/externserve"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internserve"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/lookup"
)

const ZK_SERVER = "54.219.185.237:2181"

var basePort = flag.Int("port", 1053, "base port number (DNS = base, internapi = base+1)")
var serverName = flag.String("name", "", "Name or ID of the current server.")
var serverAddr = flag.String("addr", "", "IP:Port of the current server. For example: 173.25.34.23:1054")
var zkServers = flag.String("zkservers", ZK_SERVER, "Comma-separated list of zookeeper server addresses.")

func main() {
	flag.Parse()

	externAddr := fmt.Sprintf(":%d", *basePort)
	internAddr := fmt.Sprintf(":%d", *basePort+1)

	consistentHashing := externserve.CreateConsistentHashing()
	membership := chmembership.NewMembership(
		consistentHashing,
		time.Second,
		chmembership.ServerNode{Name: *serverName, Addr: *serverAddr},
		strings.Split(*zkServers, ","),
	)

	// Initialize membership
	membership.Init()

	// Spin up external serve and internal serve routings
	go externserve.Start(externAddr, membership)
	go internserve.Start(internAddr, lookup.L2Cache)

	select {} // block forever
}
