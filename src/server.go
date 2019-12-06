package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Server struct {
	sid              int
	did              int // datacenter id
	peerDids         []int
	clientFacingPort string
	masterFacingPort string
	kvStore          map[string]string
}

func (self *Server) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
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
}
