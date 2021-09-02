package main

import (
	"eeylops/server"
	"flag"
)

func main() {
	flag.Parse()
	nm := server.NewNodeManager()
	nm.Run()
}
