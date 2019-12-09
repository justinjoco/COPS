package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

func main() {

	args := os.Args[1:6]
	fmt.Println(os.Args)
	processType := args[0]
	IntId, _ := strconv.Atoi(args[1])
	numPartitions, _ := strconv.Atoi(args[3])
	port := args[4]
	if processType == "server" {
		n, _ := strconv.Atoi(args[2])
		peerDids := make([]int, n)
		for i := 0; i < n; i++ {
			peerDids[i] = i
		}
		// start server
		clientFacingPort := strconv.Itoa(20000 + IntId)
		fmt.Println("CLIENT FACING PORT")
		fmt.Println(clientFacingPort)
		server := Server{sid: IntId, masterFacingPort: port, clientFacingPort: clientFacingPort,
			kvStore: make(map[string][]string), peerDids: peerDids}
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
