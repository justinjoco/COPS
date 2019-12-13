package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
)

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
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
		fmt.Println(peerDids)
		
		clientFacingPort := strconv.Itoa(20000 + IntId)
		
		did := IntId / 1000
		

		server := Server{sid: IntId, did: did, masterFacingPort: port, clientFacingPort: clientFacingPort,
			numPartitions: numPartitions, kvStore: make(map[string][]string), peerDids: peerDids,
			connLocalServers: make(map[int]net.Conn), localServerReaders: make(map[int]*bufio.Reader),}

		server.Run()

	} else if processType == "client" {
		did := args[2]
		IntDid, _ := strconv.Atoi(did)
		client := Client{cid: IntId, did: IntDid, masterFacingPort: port, readers: make(map[int]*bufio.Reader),
			numPartitions: numPartitions, openedServerConns: make(map[int]net.Conn), keyVersionMap: make(map[string]string), nearest: make(map[string]string)}
		client.Run()

	} else {
		fmt.Println("Invalid process type, quitting")
		os.Exit(0)
	}

}
