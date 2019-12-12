package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"sync"
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
	msgQueue           map[int]string
	connMaster			net.Conn

}

var lock = sync.RWMutex{}

func (self *Server) Run() {

	lMaster, errM := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if errM != nil {
		fmt.Println("Error listening to master!")
	}




	localFacingPort := strconv.Itoa(25000 + self.sid%1000 + 100*self.did)

	lLocal, errL := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+localFacingPort)
	if errL != nil {
		fmt.Println("Error while listening to local connection")
		fmt.Println(errL)
	}

	lClient, errC := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.clientFacingPort)
	if errC != nil {
			fmt.Println("error listeining to client")
		}

	go self.HandleLocal(lLocal)

	
	go self.ListenMaster(lMaster)

	
	self.HandleClient(lClient)


}

/*

func (self* Server) HandleNearest(){


}*/


func (self *Server) ListenMaster(lMaster net.Listener) {

	connMaster, errMC := lMaster.Accept()
	if errMC != nil {
		fmt.Println("Error while accepting connection")
	}
	self.connMaster = connMaster
	reader := bufio.NewReader(connMaster)
	//bufferNearest := make([]string,0)
	for {

		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		fmt.Println(self.sid)
		fmt.Println("MESSAGE FROM MASTER TO REPLICATE")
		fmt.Println(message)
		messageSlice := strings.Split(message, ",")

		receivedKey := messageSlice[0]
		receivedValue := messageSlice[1]
		senderDid := messageSlice[2]
		receivedVersion, _ := strconv.Atoi(messageSlice[3])
		receivedNearest:= messageSlice[4:]

		senderDidInt, _ := strconv.Atoi(senderDid)

		currentVersion := len(self.kvStore[receivedKey])
		fmt.Println("RECEIVED NEAREST")
		fmt.Println(receivedNearest)

		for _, depStr := range receivedNearest{
			dep := strings.Split(depStr, ":")
			if len(dep) < 2{
				continue
			}
			depKey, _ := strconv.Atoi(dep[0])
			depVersion := dep[1]

			//depVersionInt, _ := strconv.Atoi(dep[1])
			localId := depKey%self.numPartitions
			fmt.Println("DEP KEY, DEP VERSION, CURRENT VERSION")
			fmt.Println(depKey)
			fmt.Println(depVersion)
			fmt.Println(currentVersion)

		//	myId := self.sid%1000
			//if depVersionInt > currentVersion{
				if _, ok := self.connLocalServers[localId]; !ok {
					otherServerPort := strconv.Itoa(25000 + localId + 100*self.did) // some math  here
					connLocal, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+otherServerPort)
					if err != nil {
						fmt.Println("errro connection to local server")
					}
					self.connLocalServers[localId] = connLocal
				}
				otherServerConn := self.connLocalServers[localId]

				msgToLocal := "dep_check " + dep[0] + " " + depVersion + " " + senderDid + "\n"

				otherServerConn.Write([]byte(msgToLocal))
				// wait for dep respionse from other server
				if _, ok := self.localServerReaders[localId]; !ok {
					reader := bufio.NewReader(otherServerConn)
					self.localServerReaders[localId] = reader
				}
				localReader := self.localServerReaders[localId]
				otherServerReply, _ := localReader.ReadString('\n')
				otherServerReply = strings.TrimSuffix(otherServerReply, "\n") // string of true or false
				fmt.Println(otherServerReply)
		//	} 


		}

		fmt.Println("MY DID")
		fmt.Println(self.did)

		fmt.Println("SENDER DID")
		fmt.Println(senderDid)
		
		lock.Lock()	
		
		if receivedVersion >= currentVersion {



			if _, ok := self.kvStore[receivedKey]; !ok {
				self.kvStore[receivedKey] = []string{receivedValue+","+senderDid}
			} else {

				self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue+","+senderDid)
				
			}
		} else {

			fmt.Println("ELSE STATEMENT: HANDLING SELF")
					
			
				if _, ok := self.kvStore[receivedKey]; !ok {
					self.kvStore[receivedKey] = []string{receivedValue+","+senderDid}
				} else {
					valDidSlice := strings.Split(self.kvStore[receivedKey][len(self.kvStore[receivedKey])-1], ",")
					did, _ := strconv.Atoi(valDidSlice[1])
					if senderDidInt >= did {

					self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue+","+senderDid)
					
				}
			}
			
				
		}
		lock.Unlock()
		fmt.Println("KV STORE AFTER REPLICATE")
		fmt.Println(self.kvStore)
		
	}

}

func (self *Server) HandleClient(lClient net.Listener) {
	// handles the put_after command from the client and commits.

	connClient, errLC := lClient.Accept()
	if errLC != nil {
		fmt.Println("Error while accepting connection")
	}

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
			nearest := messageSlice[4:]

			fmt.Println("NEAREST")
			fmt.Println(nearest)
			nearestStr := strings.Join(nearest, ",")
			version := 0

			didStr := strconv.Itoa(self.did) 
			value += ","+didStr
			lock.Lock()	
			if _, ok := self.kvStore[key]; !ok {
				self.kvStore[key] = []string{value}
			} else {
				self.kvStore[key] = append(self.kvStore[key], value)
			}
			version = len(self.kvStore[key])
			lock.Unlock()
		//	self.lClock += 1


			msgToMaster := ""
			destIds := make([]string, 0)

			for _, otherDid := range self.peerDids {

				if otherDid == self.did {
					continue
				}

				destID := strconv.Itoa(otherDid*1000 + self.sid%1000)
				destIds = append(destIds, destID)


				msg := key + "," + value + "," + strconv.Itoa(self.did) + "," + strconv.Itoa(version)+ "," + nearestStr 


				msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg
				msgLength := strconv.Itoa(len(msgToMaster))
				msgToMaster = msgLength + "-" + msgToMaster

				self.connMaster.Write([]byte(msgToMaster))
			}

			latestVersion := strconv.Itoa(len(self.kvStore[key])) + "\n"

			fmt.Println("KV STORE")
			fmt.Println(self.kvStore)

			connClient.Write([]byte(latestVersion))

		case "get":

			key := messageSlice[1]
			//version, _ := strconv.Atoi(messageSlice[2])
			retVersion := ""
			retrievedValue := ""
			fmt.Println("KV STORE ")
			
		
			lock.RLock()
			fmt.Println(self.kvStore)
			retrievedValue = self.kvStore[key][len(self.kvStore[key])-1]
			retVersion = strconv.Itoa(len(self.kvStore[key]))
			lock.RUnlock()
			retValueSlice := strings.Split(retrievedValue, ",")
			retValue := retValueSlice[0]


			retMsg := retValue + " " + retVersion + "\n"
			connClient.Write([]byte(retMsg))
		}
	}
}

func (self *Server) HandleLocal(lLocal net.Listener) {
	defer lLocal.Close()

	fmt.Println("LISTENING TO LOCAL FACING PORT")
	connLocal, errL := lLocal.Accept()
	if errL != nil {
		fmt.Println("error accepting local connection")
	}
	reader := bufio.NewReader(connLocal)
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")

		messageSlice := strings.Split(message, " ")
	//	fmt.Println("RECEIVED DEP CHECK FROM LOCAL")
		command := messageSlice[0]
		switch command{
			case "dep_check":
				keyDep := messageSlice[1]
				versionDep, _ := strconv.Atoi(messageSlice[2])
				for {
					lock.RLock()
					versionNum :=len(self.kvStore[keyDep])
					_, ok := self.kvStore[keyDep]
    				lock.RUnlock()

					if !ok {
					
						continue
					} else if versionNum  == versionDep {

						connLocal.Write([]byte("resolved\n"))
						break
					} else{
						
						continue
					}
					time.Sleep(100 * time.Millisecond)
				}
			default:
				connLocal.Write([]byte("Invalid message. Need dep_check\n"))

		}
		
	}

}
