package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type KeyVersion struct {
	key        string
	versionNum int
}

type Server struct {
	sid              int
	did              int // datacenter id
	peerDids         []int
	clientFacingPort string
	masterFacingPort string
	kvStore          map[KeyVersion]string
}

func (self *Server) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	if error != nil {
		fmt.Println("Error listening to master!")
	}
	lClient, errC := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.clientFacingPort)
	if errC != nil {
		fmt.Println("error listeining to client")
	}

	self.ListenMaster(lMaster)

}

func (self *Server) ListenMaster(lMaster net.Listener) {
	defer lMaster.Close()

	connMaster, error := lMaster.Accept()
	reader := bufio.NewReader(connMaster)
	for {

		if error != nil {
			fmt.Println("Error while accepting connection")
			continue
		}

		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, " ")

	}
	connMaster.Close()
}

func (self *Server) HandleClient(lClient net.Listener) {
	// handles the put_after command from the client and commits.
	defer lClient.Close()

	connClient, error := lClient.Accept()
	reader := bufio.NewReader(connClient)
}
