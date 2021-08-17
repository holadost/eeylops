package main

import (
	"eeylops/server/hedwig"
	"flag"
)

func main() {
	flag.Parse()
	nm := hedwig.NewNodeManager()
	nm.Run()
}
