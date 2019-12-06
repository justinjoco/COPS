package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Server struct {
	sid              string
	dcid             string
	clientFacingPort string
	masterFacingPort string
	playList         map[string]string
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

func (self *Server) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	self.HandleMaster(lMaster)

}

func (self *Server) HandleMaster(lMaster net.Listener) {
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
		retMessage := ""
		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()

}
