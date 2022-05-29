package main

import (
	"flag"
	"fmt"
	"sync"

	"go.timothygu.me/stanford-cs244b-project/externserve"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internserve"
)

var basePort = flag.Int("port", 1058, "base port number (DNS = base, internapi = base+1)")

func main() {

	internAddr := fmt.Sprintf("0.0.0.0:%d", *basePort+1)

	// TODO: this should be moved
	cache := sync.Map{}

	go externserve.Start()
	go internserve.Start(internAddr, &cache)

	select {} // block forever
}
