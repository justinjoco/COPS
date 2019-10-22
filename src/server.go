package main

import (
	"fmt"
	"net"
//	"os"
	"bufio"
	"strings"
	"time"
	"strconv"
	"sort"
)

type Server struct{
	pid string
	peers []string
	masterPort string
	peerPort string
	broadcastMode bool
	alive []string 
	messages [] string

}

const(
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"

)


func (self *Server) run(){

	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST + ":" + self.masterPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST + ":" + self.peerPort)

	if error !=nil{
		fmt.Println("Error listening!")
	}

	go self.sendPeers(false, "ping")
	go self.receivePeers(lPeer)
	self.handleMaster(lMaster)



}


func (self *Server) handleMaster(lMaster net.Listener){
	defer lMaster.Close()

	connMaster, error := lMaster.Accept()
	reader := bufio.NewReader(connMaster)
	for {
		
		
		if error != nil{
			fmt.Println("Error while accepting connection")
			continue
		}

		
		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		
		
		retMessage := ""
	 	removeComma := 0
		if message == "alive"{
		 	retMessage += "alive "
		 	for _, port := range self.alive{
		 		retMessage += port + ","
		 		removeComma = 1
		 	}

		 	retMessage = retMessage[0:len(retMessage) - removeComma]
		 	lenStr := strconv.Itoa(len(retMessage))

		 	retMessage = lenStr + "-" + retMessage

		 } else if message == "get"{
		 	retMessage += "messages "
	 		for _, message := range self.messages{
	 			retMessage += message + ","
	 			removeComma = 1
	 		}

	 		retMessage = retMessage[0:len(retMessage) - removeComma]
		 	lenStr := strconv.Itoa(len(retMessage))

		 	retMessage = lenStr + "-" + retMessage

		 
		 }else{
			
			broadcastMessage := after(message, "broadcast ")
			if broadcastMessage != ""{ 
				self.messages = append(self.messages, broadcastMessage)
				self.sendPeers(true, broadcastMessage)
			}else{
				retMessage += "Invalid command. Use 'get', 'alive', or 'broadcast <message>'"
			}
		}

		connMaster.Write([]byte(retMessage))
	

	
	}

	connMaster.Close()
	 

}


func (self *Server) receivePeers(lPeer net.Listener){
	defer lPeer.Close()
	
	
	for {		
		connPeer, error := lPeer.Accept()
			
		if error != nil{
			fmt.Println("Error while accepting connection")
			continue
		}

		 message, _ := bufio.NewReader(connPeer).ReadString('\n')
		 message = strings.TrimSuffix(message, "\n")
		 if message == "ping"{
		 	connPeer.Write([]byte(self.pid))
		 }else{
		 	self.messages = append(self.messages, message)
		 }
		 connPeer.Close()
		 
		 
	}

	
}


func (self *Server) sendPeers(broadcastMode bool, message string){

	for {

		
		var tempAlive []string
		
		for _, otherPort := range self.peers{

			if otherPort != self.peerPort{
				peerConn, err := net.Dial("tcp", "127.0.0.1:" + otherPort)
			    if err != nil {
			        continue
			    }

			    
			    fmt.Fprintf(peerConn, message + "\n")
			    response, _ := bufio.NewReader(peerConn).ReadString('\n')	  
	   			tempAlive = append(tempAlive, response)
   			}
		
		}

		if broadcastMode {
			break
		}

		tempAlive = append(tempAlive, self.pid)
		sort.Strings(tempAlive)
		self.alive = tempAlive
		time.Sleep(1000 * time.Millisecond)
	}



}


func after(input string, target string) string{
	pos := strings.LastIndex(input, target)
	if pos == -1 {
		return ""
	}
	adjustedPos := pos + len(target)
	if adjustedPos >= len(input) {
		return ""
	}
	return input[adjustedPos:len(input)]
}

