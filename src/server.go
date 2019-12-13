package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
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
	connMaster         net.Conn
}

var lock = sync.RWMutex{}

func (self *Server) Run() {

	lMaster, errM := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)

	if errM != nil {
		fmt.Println("Error listening to master!")
	}

	go self.ListenMaster(lMaster)

	localFacingPort := strconv.Itoa(25000 + self.sid%1000 + 100*self.did)

	

	lLocal, errL := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+localFacingPort)
	if errL != nil {
		fmt.Println("Error while listening to local connection")
		fmt.Println(errL)
	}

	go self.HandleLocal(lLocal)

	lClient, errC := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.clientFacingPort)
	if errC != nil {
		fmt.Println("error listeining to client")
	}

	//replicateChannel := make(chan string, 100)
	//go self.Replicate(replicateChannel)





	self.HandleClient(lClient)

}



func (self* Server) Replicate(message string){

				
	messageSlice := strings.Split(message, ",")
	//fmt.Println(messageSlice)

	
	receivedKey := messageSlice[0]
	receivedValue := messageSlice[1]
	senderDid := messageSlice[2]
	receivedVersion, _ := strconv.Atoi(messageSlice[3])


	senderDidInt, _ := strconv.Atoi(senderDid)

	currentVersion := len(self.kvStore[receivedKey])

	if self.did == 2 {
		fmt.Println("TOLD TO REPLICATE")
		fmt.Println(messageSlice)
	}
	
	if len(messageSlice) > 4 {
		receivedNearest := messageSlice[4:]


		resolved := false
		
		for !resolved{
			numResolved := 0
			numNearest  := len(receivedNearest)


			for _, depStr := range receivedNearest{

				

				dep := strings.Split(depStr, ":")
			
				
				depKey, _ := strconv.Atoi(dep[0])
				depVersion := dep[1]

				localId := depKey%self.numPartitions

				if self.did == 2{
					fmt.Println("LOCAL ID")
					fmt.Println(localId)
					fmt.Println("DATA CENTER 2 DEP")
					fmt.Println(dep)
					
					fmt.Println("LENGTH OF SLICE")
					fmt.Println(len(messageSlice))

					for _, val := range receivedNearest{
						fmt.Println(val)
					}


				}

				if localId == self.sid%1000{

					keyDep := dep[0]
					versionDep, _ := strconv.Atoi(dep[1])
			
					lock.RLock()
					versionNum :=len(self.kvStore[keyDep])
					_, ok := self.kvStore[keyDep]
				//	kv:= self.kvStore
					lock.RUnlock()

					if !ok {
					//	fmt.Println("NOT OK")
						
					} else if versionNum  == versionDep {
							
						numResolved +=1
						
					} else{
				//		fmt.Println("OK BUT FAILED")
						
						
					}
					continue

				}
				/*
				fmt.Println("LOCAL")
				fmt.Println(localId)
				fmt.Println("SELF ID MOD 1000")
				fmt.Println(self.sid)
				fmt.Println(self.sid%1000)

				if localId == self.sid%1000{
					fmt.Println("TALKING TO MYSELF")
				}*/

				//depVersionInt, _ := strconv.Atoi(dep[1])
				
				
				if _, ok := self.connLocalServers[localId]; !ok {
					otherServerPort := strconv.Itoa(25000 + localId + 100*self.did) // some math  here
					if self.did == 2{
						fmt.Println("OTHER SERVER PORT")
						fmt.Println(otherServerPort)
					}
					connLocal, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+otherServerPort)
					if err != nil {
						fmt.Println("errro connection to local server")
					}

					if self.sid / 1000 == localId {
						fmt.Println("CONTACTED MYSELF")
					}
					self.connLocalServers[localId] = connLocal

				}

				msgToLocal := "dep_check " + dep[0] + " " + depVersion + "\n"
				
				otherServerConn := self.connLocalServers[localId]


				otherServerConn.Write([]byte(msgToLocal))
				// wait for dep respionse from other server
				if _, ok := self.localServerReaders[localId]; !ok {
					reader := bufio.NewReader(otherServerConn)
					self.localServerReaders[localId] = reader
				}
				localReader := self.localServerReaders[localId]
				otherServerReply, _ := localReader.ReadString('\n')
				otherServerReply = strings.TrimSuffix(otherServerReply, "\n") // string of true or false
				otherServerReplySlice := strings.Split(otherServerReply, " ")
				if self.did == 2{
					fmt.Println(otherServerReply)
				}
				if otherServerReplySlice[0] == "resolved"{
					if otherServerReplySlice[1] == dep[0] && otherServerReplySlice[2] == dep[1] {
						numResolved +=1
					}
				} else {
					break
				}
			}
				
			if numResolved == numNearest{
				resolved = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}

		}


	}

	lock.Lock()	
	//fmt.Println(currentVersion)
	if receivedVersion > currentVersion {


		if _, ok := self.kvStore[receivedKey]; !ok {
			self.kvStore[receivedKey] = []string{receivedValue + "," + senderDid}
		} else {

			self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue+","+senderDid)

		}
		
	} else {

		

		if _, ok := self.kvStore[receivedKey]; !ok {
			self.kvStore[receivedKey] = []string{receivedValue + "," + senderDid}
	
		} else {
			valDidSlice := strings.Split(self.kvStore[receivedKey][len(self.kvStore[receivedKey])-1], ",")
			did, _ := strconv.Atoi(valDidSlice[1])
			if senderDidInt >= did {
				self.kvStore[receivedKey] = append(self.kvStore[receivedKey], receivedValue+","+senderDid)
			
			}
		}

	}
	lock.Unlock()

//	fmt.Println("KV STORE AFTER REPLICATE")
//	fmt.Println(self.kvStore)

	

}





func (self *Server) ListenMaster(lMaster net.Listener) {

	connMaster, errMC := lMaster.Accept()
	if errMC != nil {
		fmt.Println("Error while accepting connection")
	}
	self.connMaster = connMaster
	reader := bufio.NewReader(connMaster)
	
	for {

		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		
		go self.Replicate(message)

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

			
			nearestStr := strings.Join(nearest, ",")
			version := 0

		
			didStr := strconv.Itoa(self.did)
			value += "," + didStr
			lock.Lock()
			if _, ok := self.kvStore[key]; !ok {
				self.kvStore[key] = []string{value}
			} else {
				self.kvStore[key] = append(self.kvStore[key], value)
			}
			version = len(self.kvStore[key])
			lock.Unlock()

			msgToMaster := ""
			destIds := make([]string, 0)

			if len(nearestStr) > 0{
				nearestStr = "," + nearestStr
			}

			for _, otherDid := range self.peerDids {

				if otherDid == self.did {
					continue
				}

				destID := strconv.Itoa(otherDid*1000 + self.sid%1000)
				destIds = append(destIds, destID)
				msg := key + "," + value  + "," + strconv.Itoa(version) + nearestStr 

				msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg
				msgLength := strconv.Itoa(len(msgToMaster))
				msgToMaster = msgLength + "-" + msgToMaster

				self.connMaster.Write([]byte(msgToMaster))
			}

			latestVersion := strconv.Itoa(len(self.kvStore[key])) + "\n"



			connClient.Write([]byte(latestVersion))

		case "get":

			key := messageSlice[1]
			//version, _ := strconv.Atoi(messageSlice[2])
			retVersion := ""
			retrievedValue := ""
			

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
		
				lock.RLock()
				versionNum :=len(self.kvStore[keyDep])
				_, ok := self.kvStore[keyDep]
			//	kv:= self.kvStore
				lock.RUnlock()

				if !ok {
				//	fmt.Println("NOT OK")
					connLocal.Write([]byte("failed\n"))
					
				} else if versionNum  == versionDep {
						
					retStr := "resolved " + keyDep + " " + messageSlice[2] +"\n"
					
					connLocal.Write([]byte(retStr))
					
				} else{
			//		fmt.Println("OK BUT FAILED")
					connLocal.Write([]byte("failed\n"))
					
				}


		default:
			connLocal.Write([]byte("Invalid message. Need dep_check\n"))
		}

	}

}
