# _ in Go
Authors: Dilip Reddy (dr589), Justin Joco (jaj263), Zhilong Li (zl242)

Slip days used(this project): _

Slip days used(total):

* Dilip (dr589): _

* Justin (jaj263): _

* Zhilong Li (zl252): _

### Automated testing 
Within the root directory of this project directory, run your test script with a master process here.
For more inpromptu script testing, run `python master.py < <testFile>.input`, which will return the test output to stdout.

### Non-deterministic/impromptu testing
Assume that you are in the root directory of this project using a terminal:
* To build, run `./build` in order to create a binary Go file, which will be created in a /bin/ folder.

For each worker process you want to run, open a new window and go to this project directory and do the following:
* Run the Go program by running `./process <id> <n> <portNum>`, which runs the executable file in the /bin/ folder from earlier.
* Use 'CTRL C' to end this specific process.

To stop all Go processes from running and destroy the DT logs, run `./stopall`.

To mimic a master process talking to a worker process, open another Terminal window and do the following:
* Use the command `nc localhost <portNum>` or `netcat localhost <portNum>` (depending on your OS) in order to open a connection with a worker process.
* Use `get`, `alive`, or `broadcast <message>` in order to get that process's message log, its record of alive peers, or to broadcast a message from that process to other peers, respectively.

Otherwise, use master.py to send your commands directly to the distributed processes.

### OS Testing Environment
Our group used OSX Mojave 10.14.x and Ubuntu Linux to compile, run, and test this project.






