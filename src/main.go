package main

import (
	"os"
	"strconv"
)


func main(){

	args :=os.Args[1:4]
	

	serverId := args[0]
	n, _ := strconv.Atoi(args[1])
	masterFacingPort :=  args[2]
	id_num, _ := strconv.Atoi(serverId)
	
	peerFacingPort := strconv.Itoa(20000+ id_num)

	var peers []string


	for i:=0 ; i < n; i++ {
		peerStr := strconv.Itoa(20000+i)
		peers = append(peers, peerStr)
	}


	server:= Server{pid: serverId, peers: peers, masterFacingPort: masterFacingPort, peerFacingPort: peerFacingPort}
	

	server.Run()


	os.Exit(0)
	
}