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
	numPartitions    int
	contextID        int
	kvStore          map[string][]string //map from key to a slice of values,
	//0th element in the slice is version 1
	latestMsgID        int //starts from 0, i.e if we receive something with 2 first, then wait until received 1, then commit both
	connLocalServers   map[int]net.Conn
	localServerReaders map[int]*bufio.Reader
	noDependency       string //"true" if no dependency issues, "false" otherwise. i.e. if this server is "blocking" then false
	msgQueue           []string
}

func (self *Server) Run() {

	lMaster, errM := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if errM != nil {
		fmt.Println("Error listening to master!")
	}

	localFacingPort := strconv.Itoa(25000 + self.sid%1000 + 100*self.did)
	fmt.Println("MY LISTENING PORT")
	fmt.Println(localFacingPort)
	lLocal, errL := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+localFacingPort)
	if errL != nil {
		fmt.Println("Error while listening to local connection")
		fmt.Println(errL)
	}
	go self.HandleLocal(lLocal)

	connMaster, errMC := lMaster.Accept()
	if errMC != nil {
		fmt.Println("Error while accepting connection")
	}

	go self.ListenMaster(connMaster)

	lClient, errC := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.clientFacingPort)
	if errC != nil {
		fmt.Println("error listeining to client")
	}

	connClient, errLC := lClient.Accept()
	if errLC != nil {
		fmt.Println("Error while accepting connection")
	}

	self.HandleClient(connClient, connMaster)

}

func (self *Server) broadcastContext() {
	// myIdx := self.sid % 1000 //the index of this server
	for i := 0; i < self.numPartitions; i++ {
		// if i != myIdx { // for all the other servers in this datacenter
		if _, ok := self.connLocalServers[i]; !ok {
			otherServerPort := strconv.Itoa(25000 + i + 100*self.did) // some math  here
			fmt.Println("OTHER SERVER PORT")
			fmt.Println(otherServerPort)
			connLocal, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+otherServerPort)
			if err != nil {
				fmt.Println("errro connection to local server")
			}
			self.connLocalServers[i] = connLocal
		}
		otherServerConn := self.connLocalServers[i]
		fmt.Println("SEDNING OVER NOW")
		fmt.Println(self.contextID)
		otherServerConn.Write([]byte(strconv.Itoa(self.contextID) + "\n"))
		// wait for dep respionse from other server
		// if _, ok := self.localServerReaders[i]; !ok {
		// 	reader := bufio.NewReader(otherServerConn)
		// 	self.localServerReaders[i] = reader
		// }
		// localReader := self.localServerReaders[i]
		// otherServerReply, _ := localReader.ReadString('\n')
		// otherServerReply = strings.TrimSuffix(otherServerReply, "\n") // string of true or false
		// if otherServerReply == "false" {
		// 	fmt.Println("DEP CHECK RETURNING FALSE")
		// 	return false
		// }

	}
}

func (self *Server) ListenMaster(connMaster net.Conn) {

	reader := bufio.NewReader(connMaster)
	for {

		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		fmt.Println(self.sid)
		fmt.Println("MESSAGE FROM MASTER TO REPLICATE")
		fmt.Println(message)
		messageSlice := strings.Split(message, ",")

		receivedKey := messageSlice[0]
		receivedValue := messageSlice[1]
		receivedPutId, _ := strconv.Atoi(messageSlice[2])
		fmt.Println("MY CONTEXT ID WHEN RECEUVED REPLICATE FROM MASYET")
		fmt.Println(self.contextID)
		if receivedPutId > self.contextID+1 {
			self.noDependency = "false" // This is purely local dependency, not considering the other servers
		} else {
			self.noDependency = "true"
		}
		//check for dependency first
		if self.noDependency == "true" {
			if _, ok := self.kvStore[receivedKey]; !ok {
				self.kvStore[receivedKey] = []string{receivedValue}
			} else {
				self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue)
			}
			self.contextID = receivedPutId
			fmt.Println("BRoadcasting my contextID, which is ")
			fmt.Println(self.contextID)
			// broadcast contextID to the other servers locally
			self.broadcastContext()

		} else { // add this message to the queue
			fmt.Println("IN THE ELSE BRNACH, ADDING TO MSGQ")
			fmt.Println(message)
			fmt.Println(self.contextID)
			self.msgQueue = append(self.msgQueue, message)
			fmt.Println(self.msgQueue)
		}

	}

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
			self.contextID, _ = strconv.Atoi(putID)
			self.broadcastContext()
			msgToMaster := ""
			destIds := make([]string, 0)

			for _, otherDid := range self.peerDids {

				if otherDid == self.did {
					continue
				}

				destID := strconv.Itoa(otherDid*1000 + self.sid%1000)
				destIds = append(destIds, destID)

				msg := key + "," + value + "," + putID
				msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg
				msgLength := strconv.Itoa(len(msgToMaster))
				msgToMaster = msgLength + "-" + msgToMaster

				connMaster.Write([]byte(msgToMaster))
			}

			latestVersion := strconv.Itoa(len(self.kvStore[key])) + "\n"

			connClient.Write([]byte(latestVersion))

		case "get":

			key := messageSlice[1]
			version, _ := strconv.Atoi(messageSlice[2])
			retVersion := ""
			retrievedValue := ""
			if version == 0 {
				retrievedValue = self.kvStore[key][len(self.kvStore[key])-1]
				retVersion = strconv.Itoa(len(self.kvStore[key]))
			} else {
				retrievedValue = self.kvStore[key][version-1]
				retVersion = strconv.Itoa(version)
			}

			retMsg := retrievedValue + " " + retVersion + "\n"
			connClient.Write([]byte(retMsg))
		}
	}
}

func (self *Server) HandleLocal(lLocal net.Listener) {
	defer lLocal.Close()
	connLocal, errL := lLocal.Accept()
	if errL != nil {
		fmt.Println("error accepting local connection")
	}
	reader := bufio.NewReader(connLocal)
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		newContextID, _ := strconv.Atoi(message)
		fmt.Println("MESSAGE FROM OTHER LOCAL")
		fmt.Println(message)
		self.contextID = newContextID
		// check q and see if can add the next message
		fmt.Println("MY MESSAGE Q")
		fmt.Println(self.msgQueue)
		for _, msgStr := range self.msgQueue { //TODO: might want to make it more robust by doing multiple rounds of this? (potentially recursive)
			msgTuple := strings.Split(msgStr, ",")
			msgKey := msgTuple[0]
			msgValue := msgTuple[1]
			msgLC, _ := strconv.Atoi(msgTuple[2])
			// if self.contextID+1 == msgLC {
			if _, ok := self.kvStore[msgKey]; !ok {
				self.kvStore[msgKey] = []string{msgValue}
			} else {
				self.kvStore[msgKey] = append(self.kvStore[msgKey], msgValue)
			}
			self.contextID = msgLC
			self.broadcastContext()
			// }
		}
		// if message == "dep_check" { //carry out local dep check
		// connLocal.Write([]byte(self.noDependency + "\n"))
		// } else {
		// 	fmt.Println("invalid message received from the other local server")
		// }
	}

}
