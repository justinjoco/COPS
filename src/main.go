package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func main() {

	args := os.Args[1:5]
	processType := args[0]
	IntId, _ := strconv.Atoi(args[1])
	numPartitions, _ := strconv.Atoi(args[3])
	port := args[5]
	if processType == "server" {
		n, _ := strconv.Atoi(args[2])
		peerDids := make([]int, n)
		for i := 0; i < n; i++ {
			peerDids[i] = i
		}
		// start server
		server := Server{sid: IntId, masterFacingPort: port, kvStore: make(map[string]string), peerDids: peerDids}
		server.Run()
	} else if processType == "client" {
		did := args[2]
		IntDid, _ := strconv.Atoi(did)
		client := Client{cid: IntId, did: IntDid, masterFacingPort: port,
			numPartitions: numPartitions, openedServerConns: make(map[int]net.Conn)}
		client.Run()
	} else {
		fmt.Println("Invalid process type, quitting")
		os.Exit(0)
	}

}
