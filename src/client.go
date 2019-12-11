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
	keyVersionMap map[string]string
	readers       map[int]*bufio.Reader
}


func (self *Client) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	//lServer, err := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+strconv.Itoa(self.cid)) //client's port that is exposed to its own partitions

	if error != nil {
		fmt.Println("Error listening!")
	}
	connMaster, error := lMaster.Accept()
	if error != nil {
		fmt.Println("Error while client accepting master connection")
	}

	self.HandleMaster(connMaster)
}

func (self *Client) HandleMaster(connMaster net.Conn) {
	//defer lMaster.Close()

	
	reader := bufio.NewReader(connMaster)
	for {

		message, _ := reader.ReadString('\n')
		fmt.Println("REQUEST FROM MASTER " + message)
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
			if _, ok := self.readers[serverID]; !ok {
				self.readers[serverID] = bufio.NewReader(self.openedServerConns[serverID])
			}
			versionStr, _ := self.readers[serverID].ReadString('\n')
			versionStr = strings.TrimSuffix(versionStr, "\n")
			self.keyVersionMap[key] = versionStr
			retMsg := "putResult success"
			msgLength := len(retMsg)
			retMessage := strconv.Itoa(msgLength) + "-" +retMsg
	
			connMaster.Write([]byte(retMessage))

		case "get":

			key := messageSlice[1]
			intKey, _ := strconv.Atoi(key)
			serverID := intKey%self.numPartitions + self.did*1000
			fmt.Println(self.cid)
			fmt.Println(self.keyVersionMap)
			msgToServer := "get " + key + " " + self.keyVersionMap[key] + "\n"
			fmt.Fprintf(self.openedServerConns[serverID], msgToServer)
			//need to wait response from server
			if _, ok := self.readers[serverID]; !ok {
				self.readers[serverID] = bufio.NewReader(self.openedServerConns[serverID])
			}


			value, _ := self.readers[serverID].ReadString('\n')
			value = strings.TrimSuffix(value, "\n")
			retMsg := "getResult " + key + " " + value 
			msgLength := len(retMsg)
			retMessage := strconv.Itoa(msgLength) + "-" + retMsg 
		
			connMaster.Write([]byte(retMessage))
		}
	}
}
