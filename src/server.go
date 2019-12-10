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
	localFacingPort string
	lClock			int
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
	defer lClient.Close()
	if errC != nil {
		fmt.Println("error listeining to client")
	}
	connClient, error := lClient.Accept()
	if error != nil {
		fmt.Println("Error while accepting connection")
	}

	//lLocal, errL := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.localFacingPort)


	go self.ListenMaster(connMaster)
	//go self.HandleLocal(lLocal)
	self.HandleClient(connClient, connMaster)

}

func (self *Server) ListenMaster(connMaster net.Conn) {

	reader := bufio.NewReader(connMaster)
	for {
		
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		fmt.Println("MESSAGE FROM MASTER TO REPLICATE")
		fmt.Println(message)
		messageSlice := strings.Split(message, ",")
		fmt.Println(messageSlice)
		receivedKey := messageSlice[0]
		receivedValue := messageSlice[1]
		receivedLC, _ := strconv.Atoi(messageSlice[2])
		if _, ok := self.kvStore[receivedKey]; !ok {
			self.kvStore[receivedKey] = []string{receivedValue}
		} else {
			self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue)
		}
		if receivedLC > self.lClock{
			self.lClock = receivedLC
		}
		self.lClock += 1

		fmt.Println(self.kvStore)
	}
	//connMaster.Close()
}

func (self *Server) HandleClient(connClient net.Conn, connMaster net.Conn) {
	// handles the put_after command from the client and commits.
	reader := bufio.NewReader(connClient)
	for {
		message, _ := reader.ReadString('\n')
	//	fmt.Println("MESSAGE FROM CLIENT " + message)
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
			self.lClock += 1
			msgToMaster := ""
			for _, otherDid := range self.peerDids {
				
				if otherDid == self.did {
					fmt.Println("DON'T SEND TO SELF")
					fmt.Println(self.did)
					continue
				}
	//			fmt.Println(self.peerDids)
				destID := strconv.Itoa(otherDid*1000 + self.sid%1000)
				fmt.Println("REPLICATE MESSAGE TO ")
				fmt.Println("DEST ID")
				fmt.Println(destID)
				msg := key + "," + value + "," + strconv.Itoa(self.lClock)
				msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg 
				msgLength := strconv.Itoa(len(msgToMaster))
				msgToMaster = msgLength + "-" + msgToMaster
				fmt.Println(msgToMaster)
				fmt.Println(self.kvStore)
				//fmt.Fprintf(connMaster, msgToMaster)
				connMaster.Write([]byte(msgToMaster))
			}
			latestVersion := strconv.Itoa(len(self.kvStore[key])) + "\n"
			
			connClient.Write([]byte(latestVersion))

		case "get":
			fmt.Println("SERVER ID")
			fmt.Println(self.sid)
			fmt.Println("KV STORE")
			fmt.Println(self.kvStore)
			key := messageSlice[1]
			version, _ := strconv.Atoi(messageSlice[2])
			retrievedValue := self.kvStore[key][version-1]
			retMsg := retrievedValue + "\n"
			connClient.Write([]byte(retMsg))
		}
	}
}
/*
func (self *Server) HandleLocal(lLocal net.Listener) {
	defer lLocal.Close()
	for {
		connLocal, errL := lLocal.Accept()
		if errL != nil {
			fmt.Println("error listeining to client")
		}
	
	}
}*/
