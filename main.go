package main

import (
	"flag"
	"fmt"

	"go.timothygu.me/stanford-cs244b-project/externserve"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/internserve"
	"go.timothygu.me/stanford-cs244b-project/internal/pkg/lookup"
)

var basePort = flag.Int("port", 1053, "base port number (DNS = base, internapi = base+1)")

func main() {

	externAddr := fmt.Sprintf(":%d", *basePort)
	internAddr := fmt.Sprintf(":%d", *basePort+1)

	go externserve.Start(externAddr)
	go internserve.Start(internAddr, lookup.L2Cache)

	select {} // block forever
}
