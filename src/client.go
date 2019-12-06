package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	cid              string
	did              string
	masterFacingPort string
	serverFacingPort string //listens to a server
}

func (self *Client) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	self.HandleMaster(lMaster)
}

func (self *Client) HandleMaster(lMaster net.Listener) {
	defer lMaster.Close()

	connMaster, error := lMaster.Accept()
	reader := bufio.NewReader(connMaster)
	for {

		if error != nil {
			fmt.Println("Error while client accepting master connection")
			continue
		}

		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, " ")
		command := messageSlice[0]
		retMessage := ""
		switch command {
		case "put":

		case "get":

		}
	}
}
