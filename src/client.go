/*
client.go
Program for client library process of COPS
*/

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
	numPartitions int
	keyVersionMap map[string]string
	readers       map[int]*bufio.Reader
	nearest			map[string]string

}

//Run client process; set up master facing port
func (self *Client) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	
	if error != nil {
		fmt.Println("Error listening!")
	}
	connMaster, error := lMaster.Accept()
	if error != nil {
		fmt.Println("Error while client accepting master connection")
	}

	self.HandleMaster(connMaster)
}

//Handle master connections, which will be put and get requests
func (self *Client) HandleMaster(connMaster net.Conn) {
	
	reader := bufio.NewReader(connMaster)
	for {

		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, " ")
		command := messageSlice[0]

		//Retrieve nearest dependencies; send nearest along with put request to partition
		//Receive acknowledged put request; clear nearest dep and input it put request
		//Acknowledge master that put was success
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


			
				nearestStr := ""

				for nearKey, nearVersion := range self.nearest{
					nearestStr += nearKey + ":" + nearVersion + " "
				}

				
				msgToServer := "put " + key + " " + value + " " + putID + " " + strings.TrimSpace(nearestStr) +"\n"


				fmt.Fprintf(self.openedServerConns[serverID], msgToServer)
				//TODO: need to wait ack from server
				if _, ok := self.readers[serverID]; !ok {
					self.readers[serverID] = bufio.NewReader(self.openedServerConns[serverID])
				}
				versionStr, _ := self.readers[serverID].ReadString('\n')
				versionStr = strings.TrimSuffix(versionStr, "\n")
				self.nearest = make(map[string]string)

				self.keyVersionMap[key] = versionStr

				self.nearest[key] = versionStr

				retMsg := "putResult success"
				msgLength := len(retMsg)
				retMessage := strconv.Itoa(msgLength) + "-" + retMsg

				connMaster.Write([]byte(retMessage))

			//Get value from partition based on key
			//Add get request into nearest dep list
			//Acknowledge requester with <key, value> pair
			case "get":

				key := messageSlice[1]
				intKey, _ := strconv.Atoi(key)
				serverID := intKey%self.numPartitions + self.did*1000
				
				msgToServer := "get " + key + " latest"  + "\n"

			
				//need to wait response from server
				if _, ok := self.openedServerConns[serverID]; !ok {
					serverSendPort := 20000 + serverID

					serverConn, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+strconv.Itoa(serverSendPort))
					if err != nil {
						fmt.Println("error while dialing server port")
					}
					self.openedServerConns[serverID] = serverConn
				}
				fmt.Fprintf(self.openedServerConns[serverID], msgToServer)

				if _, ok := self.readers[serverID]; !ok {
					self.readers[serverID] = bufio.NewReader(self.openedServerConns[serverID])
				}

				serverMsg, _ := self.readers[serverID].ReadString('\n')
				serverMsg = strings.TrimSuffix(serverMsg, "\n")
				serverMsgSlice := strings.Split(serverMsg, " ")
				value := serverMsgSlice[0]
				version := serverMsgSlice[1]
				// update key version map
				self.keyVersionMap[key] = version

				
				self.nearest[key] = version

				retMsg := "getResult " + key + " " + value
				msgLength := len(retMsg)
				retMessage := strconv.Itoa(msgLength) + "-" + retMsg

				connMaster.Write([]byte(retMessage))
		}
	}
}
