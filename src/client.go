package main

import (
	"fmt"
	"net"
)

type Client struct {
	cid              string
	did              string
	masterFacingPort string
	serverFacingPort string //listens to a server
}

func (self *Replica) Run(replicaLeaderChannel chan string) {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lCommander, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.commanderFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}
	connMaster, error := lMaster.Accept()

	go self.HandleCommander(lCommander, connMaster, replicaLeaderChannel) // TO listen to decisions by other process's commanders
	self.SyncDecisions(replicaLeaderChannel)
	self.HandleMaster(connMaster, replicaLeaderChannel)

}
