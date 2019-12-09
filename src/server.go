package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type Server struct {
	sid              int
	did              int   // datacenter id
	peerDids         []int // Shouldn't include own!
	clientFacingPort string
	masterFacingPort string
	kvStore          map[string][]string //map from key to a slice of values,
	//0th element in the slice is version 1
}

func (self *Server) Run() {

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	defer lMaster.Close()
	if error != nil {
		fmt.Println("Error listening to master!")
	}
	connMaster, error := lMaster.Accept()
	if error != nil {
		fmt.Println("Error while accepting connection")
	}
	lClient, errC := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.clientFacingPort)
	if errC != nil {
		fmt.Println("error listeining to client")
	}
	connClient, error := lClient.Accept()
	if error != nil {
		fmt.Println("Error while accepting connection")
	}
	self.ListenMaster(connMaster)
	self.HandleClient(connClient, connMaster)

}

func (self *Server) ListenMaster(connMaster net.Conn) {

	reader := bufio.NewReader(connMaster)
	for {
		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, ",")
		receivedKey := messageSlice[0]
		receivedValue := messageSlice[1]
		if _, ok := self.kvStore[receivedKey]; !ok {
			self.kvStore[receivedKey] = []string{receivedValue}
		} else {
			self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue)
		}
	}
	connMaster.Close()
}

func (self *Server) HandleClient(connClient net.Conn, connMaster net.Conn) {
	// handles the put_after command from the client and commits.
	reader := bufio.NewReader(connClient)
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, " ")
		command := messageSlice[0]
		switch command {
		case "put":
			key := messageSlice[1]
			value := messageSlice[2]
			putID := messageSlice[3]
			if _, ok := self.kvStore[key]; !ok {
				self.kvStore[key] = []string{value}
			} else {
				self.kvStore[key] = append(self.kvStore[key], value)
			}
			msgToMaster := ""
			for _, otherDid := range self.peerDids {
				if otherDid == self.did {
					continue
				}
				destID := strconv.Itoa(otherDid*1000 + self.sid)
				msg := key + "," + value
				msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg
				msgLength := strconv.Itoa(len(msgToMaster))
				msgToMaster = msgLength + "-" + msgToMaster
				connMaster.Write([]byte(msgToMaster))
			}

		case "get":

		}
	}
}
