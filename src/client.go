package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	cid               int
	did               int
	masterFacingPort  string
	serverFacingPort  string           //listens to a server
	openedServerConns map[int]net.Conn // a map holding the connections to servers
	// that have already been opened.
	numPartitions int
	keyVersionMap map[string]int
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

func (self *Client) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lServer, err := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+strconv.Itoa(self.cid)) //client's port that is exposed to its own partitions

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
		switch command {
		case "put":
			key := messageSlice[1]
			intKey, _ := strconv.Atoi(messageSlice[1])
			value := messageSlice[2]
			putID := messageSlice[3]
			// calculate serverID from key
			serverID := intKey%self.numPartitions + self.did*1000
			// first check if the connection has already been opened
			if _, ok := self.openedServerConns[serverID]; !ok {
				serverSendPort := 20000 + serverID
				serverConn, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+strconv.Itoa(serverSendPort))
				if err != nil {
					fmt.Println("error while dialing server port")
				}
				self.openedServerConns[serverID] = serverConn
			}
			msgToServer := "put " + key + " " + value + " " + putID + "\n"
			fmt.Fprintf(self.openedServerConns[serverID], msgToServer)
			//TODO: need to wait ack from server
			versionStr, _ := bufio.NewReader(self.openedServerConns[serverID]).ReadString('\n')
			intVersionNum, _ := strconv.Atoi(versionStr)
			self.keyVersionMap[key] = intVersionNum
			msgLength := len("putResult success")
			retMessage := strconv.Itoa(msgLength) + "-putResult success"
			connMaster.Write([]byte(retMessage))
		case "get":
			key := messageSlice[1]
		}
	}
}