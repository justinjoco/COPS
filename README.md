# Clusters of Order-Preserving Servers (COPS) in Go
Authors: Dilip Reddy (dr589), Justin Joco (jaj263), Zhilong Li (zl242)

Slip days used(this project): 0  

Slip days used(total):

* Dilip (dr589): 8

* Justin (jaj263): 2

* Zhilong Li (zl242): 2

## General Overview
COPS is a distributed data store that shards keys amongst replicated partitions in multiple clusters (datacenters). This data store maintains the properties of ALPS (Availability, low Latency, Partition-tolerance, high Scalability) datastores whilst maintaining causal+ consistency. Our implementation follows descriptions and specifications in "Don’t Settle for Eventual: Scalable Causal Consistency for Wide-Area Storage with COPS" by Wyatt Lloyd, Michael J. Freedman, Michael Kaminsky, and David G. Andersen.

Causal+ consistency is defined as causal consistency with convergent conflict handling, which is more powerful than eventually consistent stores. Such consistency is especially useful for applications that need stronger consistency properties, like bank accounts.

The COPS data store comprises of two main entities: the data store and the client library. Keys are sharded amongst partitions in a particular cluster, and every key-value pair is replicated across all other clusters. For puts, the client library calculates to which a key is to be sent to, calculates the nearest dependencies for a given put operation, then sends the key,value pair to the appropriate partition in its local cluster. Afterwards, this partition tells its replicas in all other clusters to replicate the put operation. For gets, the client library asks the appropriate partition in its local datacenter for the key,value pair, then sends it back to the requester.



### Automated testing 
Within the root directory of this project directory, run your test script with a master process here.
For more inpromptu script testing, run `python master.py < <testFile>.input`, which will return the test output to stdout.

### Non-deterministic/impromptu testing
Assume that you are in the root directory of this project using a terminal:
* To build, run `./build` in order to create a binary Go file, which will be created in a /bin/ folder.

For each worker process you want to run, open a new window and go to this project directory and do the following:
* Run a client process by running `./process client <id> <did> <s> <portNum>`, which runs the executable file in the /bin/ folder from earlier. To run a server process, run `./process server <id> <n> <s> <portNum>`. 
* Use 'CTRL C' to end this specific process.

To stop all Go processes from running, run `./stopall`.

Use master.py to send your commands directly to the distributed processes.

### OS Testing Environment
Our group used OSX Mojave 10.14.x and Ubuntu Linux to compile, run, and test this project.












