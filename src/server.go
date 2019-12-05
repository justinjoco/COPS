package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Server struct {
	sid              string
	dcid             string
	clientFacingPort string
	masterFacingPort string
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

func (self *Server) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.peerFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	go self.Heartbeat(false, "ping")
	go self.ReceivePeers(lPeer)
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
		command := messageSlice[0]

		retMessage := ""
		removeComma := 0
		switch command {
		case "alive":
			retMessage += "alive "
			for _, port := range self.alive {
				retMessage += port + ","
				removeComma = 1
			}
			retMessage = retMessage[0 : len(retMessage)-removeComma]
			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		case "get":
			retMessage += "messages "
			for _, message := range self.messages {
				retMessage += message + ","
				removeComma = 1
			}
			retMessage = retMessage[0 : len(retMessage)-removeComma]
			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		default:
			broadcastMessage := messageSlice[1]
			if broadcastMessage != "" {
				self.messages = append(self.messages, broadcastMessage)
				self.Heartbeat(true, broadcastMessage)
			} else {
				retMessage += "Invalid command. Use 'get', 'alive', or 'broadcast <message>'"
			}
		}

		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()

}

func (self *Server) ReceivePeers(lPeer net.Listener) {
	defer lPeer.Close()

	for {
		connPeer, error := lPeer.Accept()

		if error != nil {
			fmt.Println("Error while accepting connection")
			continue
		}

		message, _ := bufio.NewReader(connPeer).ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		if message == "ping" {
			connPeer.Write([]byte(self.pid))
		} else {
			self.messages = append(self.messages, message)
		}
		connPeer.Close()

	}

}
