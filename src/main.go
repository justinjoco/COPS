package main

import (
	"fmt"
	"os"
)

func main() {

	args := os.Args[1:5]
	processType := args[0]
	Id := args[1]
	s := args[3]
	port := args[5]
	if processType == "server" {
		n := args[2]
		// start server
		server := Server{sid: Id, masterFacingPort: port}
		server.Run()
	} else if processType == "client" {
		did := args[2]
		client := Client{cid: Id, did: did, masterFacingPort: port}
		client.Run()
	} else {
		fmt.Println("Invalid process type, quitting")
		os.Exit(0)
	}

}
