# _ in Go
Authors: Dilip Reddy (dr589), Justin Joco (jaj263), Zhilong Li (zl242)

Slip days used(this project): 0  

Slip days used(total):

* Dilip (dr589): 4

* Justin (jaj263): 2

* Zhilong Li (zl252): 2

## General Overview
TBA


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

Use master.py to send your commands directly to the distributed processes.

### OS Testing Environment
Our group used OSX Mojave 10.14.x and Ubuntu Linux to compile, run, and test this project.












