/*
server.go
Program for data store partition of COPS
*/
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
	did              int   
	peerDids         []int 
	clientFacingPort string
	masterFacingPort string
	numPartitions    int
	kvStore          map[string]string
	connLocalServers   map[int]net.Conn
	localServerReaders map[int]*bufio.Reader	
	connMaster         net.Conn
	keyClockMap      map[string][]int
}

//Prevent concurrent reads and writes to KV store and my saved TCP connections
var kvLock = sync.RWMutex{}
var connLocalLock = sync.RWMutex{}
var readerLock = sync.RWMutex{}
var clockLock = sync.RWMutex{}

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

	self.HandleClient(lClient)

}


//Process replication message; commit if dependencies are fulfilled and the received version is ahead
//May or may not commit if received version is same as mine
func (self* Server) Replicate(message string){
			
	messageSlice := strings.Split(message, ",")	
	receivedKey := messageSlice[0]
	receivedValue := messageSlice[1]
	senderDid, _ := strconv.Atoi(messageSlice[2])
	receivedVersion, _ := strconv.Atoi(messageSlice[3])

	clock, ok := self.keyClockMap[receivedKey]
	currentVersion := 0
	currentDid := 0
	if ok {
		currentVersion = clock[0]
		currentDid = clock[1]
	} 

	fmt.Println("MESSAGE TO REPLICATE")
	fmt.Println(messageSlice)

	fmt.Println("RECEIVED KEY")
	fmt.Println(receivedKey)

	fmt.Println("RECEIVED VERSION")
	fmt.Println(currentVersion)

	fmt.Println("CURRENT VERSION")
	fmt.Println(currentVersion)
	

	//If there are nearest dependencies
	if len(messageSlice) > 4 {
		receivedNearest := messageSlice[4:]
		resolved := false
		
		fmt.Println("NEAREST DEPENDENCIES")
		fmt.Println(receivedNearest)
		//Check if all dependencies are resolved
		for !resolved{
			numResolved := 0
			numNearest  := len(receivedNearest)



			for _, depStr := range receivedNearest{

				
				dep := strings.Split(depStr, ":")		
				depKey, _ := strconv.Atoi(dep[0])
				depVersion := dep[1]
				localId := depKey%self.numPartitions

				//If key dependency belongs to me, check if my current version number is up-to-date or ahead
				//Resolved if received version is out of date or my current version is up-to-date
				if localId == self.sid%1000{

					keyDep := dep[0]
					versionDep, _ := strconv.Atoi(dep[1])
			
					clockLock.RLock()
					versionNum := 0
					clockDep, ok := self.keyClockMap[keyDep]
					if ok {
						versionNum = clockDep[0]
					}
					clockLock.RUnlock()

					if versionNum  >= versionDep {
						numResolved +=1	
					} 
					continue

				}
				
				//Ping server that the key belongs to; save connection if it hasn't been already
				connLocalLock.Lock()
				if _, ok := self.connLocalServers[localId]; !ok {
					otherServerPort := strconv.Itoa(25000 + localId + 100*self.did) // some math  here
					
					connLocal, err := net.Dial(CONNECT_TYPE, CONNECT_HOST+":"+otherServerPort)
					if err != nil {
						fmt.Println("errro connection to local server")
					}

					
					self.connLocalServers[localId] = connLocal

				}
				connLocalLock.Unlock()
				
				//Do a dependency check on the local server
				msgToLocal := "dep_check " + dep[0] + " " + depVersion + "\n"	

				connLocalLock.RLock()
				otherServerConn := self.connLocalServers[localId]
				connLocalLock.RUnlock()

				otherServerConn.Write([]byte(msgToLocal))
				
				//Read server connection stream; save reader if it hasn't been already
				readerLock.Lock()
				if _, ok := self.localServerReaders[localId]; !ok {
					reader := bufio.NewReader(otherServerConn)
					self.localServerReaders[localId] = reader
				}
				readerLock.Unlock()

				readerLock.RLock()
				localReader := self.localServerReaders[localId]
				readerLock.RUnlock()

				//Read server's reply
				otherServerReply, _ := localReader.ReadString('\n')
				otherServerReply = strings.TrimSuffix(otherServerReply, "\n") // string of true or false
				otherServerReplySlice := strings.Split(otherServerReply, " ")
				
				//If replied with resolved with appropriate dependencies returned, increase num resolved
				//Break out of loop if we receive a "failed" message
				if otherServerReplySlice[0] == "resolved"{

					if otherServerReplySlice[1] == dep[0] && otherServerReplySlice[2] == dep[1] {
						numResolved +=1
					}

				} else {
					break
				}
			}
				
			//If all deps are resolved, leave loop to see if we should commit the replication
			//Otherwise, continue checking for dependency resolution
			if numResolved == numNearest{
				resolved = true
			} else {
				time.Sleep(100 * time.Millisecond)
			}

		}

	}

	//If received key,value,version is ahead of me, commit the key,value,version
	kvLock.Lock()	
	if receivedVersion > currentVersion {


		self.kvStore[receivedKey] = receivedValue
		self.keyClockMap[receivedKey] = []int{receivedVersion , senderDid}
		
		
	} else {
		
		// Settle tiebreak, which is when my version and the received version are the same
		// Get the did of the latest version of the received key in my data store
		// If the sender's did is greater than or equal to my latest version's did, commit the replication
		if receivedVersion == currentVersion {
			
			if senderDid >= currentDid {
				self.kvStore[receivedKey] = receivedValue
				self.keyClockMap[receivedKey] = []int{receivedVersion , senderDid}
			}
		}
		//Ignore if received version is out of date with mine (ie. is less than my current version)
		

	}
	kvLock.Unlock()

	fmt.Println("KV STORE AFTER REPLICATE")
	fmt.Println(self.kvStore)
	fmt.Println(self.keyClockMap)

	

}




//Listen to master facing port for replication puts
func (self *Server) ListenMaster(lMaster net.Listener) {

	connMaster, errMC := lMaster.Accept()
	if errMC != nil {
		fmt.Println("Error while accepting connection")
	}
	self.connMaster = connMaster
	reader := bufio.NewReader(connMaster)	

	//Always listen to master; let goroutine handle actual put operation
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		go self.Replicate(message)
	}

}

// handles the put_after command from the client and commits.
func (self *Server) HandleClient(lClient net.Listener) {
	

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
			//Adds key value pair into data store
			case "put":
				key := messageSlice[1]
				value := messageSlice[2]
				putID := messageSlice[3]
				nearest := messageSlice[4:]

				nearestStr := strings.Join(nearest, ",")
			
			
				didStr := strconv.Itoa(self.did)
			

				//Store key, value pair based on scheme: <key, value, did>
				kvLock.Lock()
				self.kvStore[key] = value
				currentClock, ok := self.keyClockMap[key]

				fmt.Println("OK OR NOT OK")
				fmt.Println(ok)

				if !ok {

					self.keyClockMap[key] = []int{1, self.did}
				}else{
					self.keyClockMap[key] = []int{currentClock[0]+1, self.did}
				}
				kvLock.Unlock()
				fmt.Println(self.keyClockMap[key][0])
				version := self.keyClockMap[key][0]
				msgToMaster := ""
				destIds := make([]string, 0)

				if len(nearestStr) > 0{
					nearestStr = "," + nearestStr
				}

				//Replicate put request to cprresponding partition in all clusters
				//Send nearest dependencies (received from client) to partitions
				for _, otherDid := range self.peerDids {

					if otherDid == self.did {
						continue
					}

					destID := strconv.Itoa(otherDid*1000 + self.sid%1000)
					destIds = append(destIds, destID)
					msg := key + "," + value  + "," + didStr + "," + strconv.Itoa(version) + nearestStr 

					msgToMaster = "route " + strconv.Itoa(self.sid) + " " + destID + " " + putID + " " + msg
					msgLength := strconv.Itoa(len(msgToMaster))
					msgToMaster = msgLength + "-" + msgToMaster

					self.connMaster.Write([]byte(msgToMaster))
				}

				//Acknowledge client with newly calculated version number (ie Lamport clock)of key 
				latestVersion := strconv.Itoa(version) + "\n"

				connClient.Write([]byte(latestVersion))

			//Retrieve value from data store based on key
			//Return value and the key's latest version number to the client library
			case "get":

				key := messageSlice[1]

				retVersion := ""
				retrievedValue := ""
				

				kvLock.RLock()
				retrievedValue = self.kvStore[key]
				retVersion = strconv.Itoa(self.keyClockMap[key][0])
				kvLock.RUnlock()
				
				retMsg := retrievedValue + " " + retVersion + "\n"
				connClient.Write([]byte(retMsg))
		}
	}
}

//Handle connections from local datacenter
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
		messageSlice := strings.Split(message, " ")
		command := messageSlice[0]

		switch command{

			//Check local datastore for dependency resolution
			case "dep_check":
				keyDep := messageSlice[1]
				versionDep, _ := strconv.Atoi(messageSlice[2])
		
				clockLock.RLock()
				clock, ok := self.keyClockMap[keyDep]
				clockLock.RUnlock()

				//If current version number is up-to-date or ahead of dependency, acknowledge with "resolved"
				//Otherwise, reply with "failed"
				if ok && clock[0]  >= versionDep {		
					retStr := "resolved " + keyDep + " " + messageSlice[2] +"\n"	
					connLocal.Write([]byte(retStr))	
				} else{
					connLocal.Write([]byte("failed\n"))				
				}


			default:
				connLocal.Write([]byte("Invalid message. Need dep_check\n"))
		}

	}

}
