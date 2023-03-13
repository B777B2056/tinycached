package main

import "tinycached/network"

func main() {
	svr := network.CacheServer{}
	svr.Run(8888)
}
