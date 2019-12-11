#!/usr/bin/env python
"""
The master program for CS5414 COPS project.
"""

import os
import signal
import subprocess
import sys
import time
import traceback
from socket import SOCK_STREAM, socket, AF_INET
from threading import Thread, Lock

address = 'localhost'
threads = {}

# List of messages that have currently been blocked
# Map (put id, PutOperation)
blocked_messages= {}
# List of put ids to block
blocked_ids = {}
# True if waiting for an ack
wait_for_ack = False
# List of started processes
started_processes = {}
# Blocked lock
block_lock = Lock()

debug = False

# Class for a put operation
class PutOperation:
    def __init__(self, srcId, destId, putId, msg):
        self.srcId = srcId
        self.destId = destId
        self.putId = putId
        self.msg = msg

# Forwards message
def forwardMsg(o):
    send(o.destId, o.msg, block=False)



class ClientHandler(Thread):
    def __init__(self, index, address, port, process):
        Thread.__init__(self)
        self.daemon = True
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((address, port))
        self.buffer = ""
        self.valid = True
        self.process = process


    def run(self):
        global threads, wait_for_ack
        msgLen = 0
        while self.valid:
            try:
               # print(self.buffer)
                # No message is being processed
                if (msgLen == 0):
                   # print(self.sock.getsockname())
                    if (len(self.buffer) < 4):
                        # Not enough for an integer,
                        # must read more

                        data = self.sock.recv(1024)
                        self.buffer+=data
                    else:
                        # Now extract header

                        (header,msg)= self.buffer.split("-",1)
                        msgLen = int(header)
                        self.buffer = msg
                else:
                    # Message has not yet been fully received
                    if (len(self.buffer)<msgLen):
                        data = self.sock.recv(1024)
                        self.buffer+=data
                    else:
                        msg,rest = self.buffer[0:msgLen],self.buffer[msgLen:]
                        self.buffer= rest
                        self.handleMsg(msg)
                        msgLen = 0
            except Exception,e:
                print (traceback.format_exc())
                self.valid = False
                del threads[self.index]
                self.sock.close()
                break


    def handleMsg(self,msg):
        global wait_for_ack, blocked_messages, blocked_ids, block_lock
        s = msg.split()
        # Client replies result of get operation
        if s[0] == 'getResult':
            wait_for_ack = False
            print msg.split(None,1)[1]
        elif s[0] == 'putResult':		
            wait_for_ack = False		
        # Message that should be routed
        elif s[0] == 'route':
            srcId = s[1]
            destId = s[2]
            putId = s[3]
            msg = " ".join(s[4:])
            o = PutOperation(srcId,destId,putId,msg)
            block_lock.acquire()
            # Check if message is not currently blocked
            if putId not in blocked_ids:
                forwardMsg(o)
            else :
                # Messasge is blocked, but not this destination
                if destId not in blocked_ids[putId]:
                    forwardMsg(o)
                else:
                    # Add message to blocked id
                    # First message, need to create dict
                    if putId not in blocked_messages:
                        blocked_messages[putId]= {}
                    blocked_messages[putId][destId] = o
            block_lock.release()
        else:
            print 'WRONG MESSAGE:', s

    def kill(self):
        if self.valid:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
            except:
                pass
            self.close()

    def send(self, s):
        if self.valid:
            self.sock.send(str(s) + '\n')

    def close(self):
        try:
            self.valid = False
            self.sock.close()
        except:
            pass


def send(index, data, set_wait=False, block=True):
    global threads, wait_for_ack
    while wait_for_ack and block:
        time.sleep(0.01)
    pid = int(index)
    if set_wait:
        wait_for_ack= True
    threads[pid].send(data)


def exit(is_exit=False):
    global threads, wait_for_ack

    wait = wait_for_ack
    wait = wait and (not is_exit)
    while wait:
        time.sleep(0.01)
        wait = wait_for_ack

    time.sleep(2)
    for k in threads:
        threads[k].kill()
    subprocess.Popen(['./stopall'], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
    sys.stdout.flush()
    time.sleep(1)
    if is_exit:
        os._exit(0)
    else:
        sys.exit(0)


def timeout():
    time.sleep(120)
    exit(True)


def main():
    global threads, wait_for_ack, started_processes, debug, blocked_messages, blocked_ids
    timeout_thread = Thread(target=timeout, args=())
    timeout_thread.daemon = True
    timeout_thread.start()

    while True:
        line = ''
        try:
            line = sys.stdin.readline()
        except:  # keyboard exception, such as Ctrl+C/D
            exit(True)
        if line == '':  # end of a file
            exit()
        line = line.strip()  # remove trailing '\n'
        print(line)
        if line == 'exit':  # exit when reading 'exit' command
            while wait_for_ack:
                print("Wait for ack")
                time.sleep(0.1)
          #  print("Wait for ack is false")
            exit()
        if len(line) == 0:
            continue
        # Extracts command type only
        sp1 = line.split(None, 1)
        # Extracts all individual arguments
        sp2 = line.split()
        pid = int(sp2[0])  #  first field is pid
        cmd = sp2[1]  # second field is command
        if cmd == 'startServer':
            port = int(sp2[4])
            if pid not in started_processes:
                started_processes[pid] = True
            else:
                print "Process already started"
                exit()
            # start the process
            if debug:
                process = subprocess.Popen(['./process', 'server', str(pid), sp2[2], sp2[3], sp2[4]], preexec_fn=os.setsid)
            else:
                process = subprocess.Popen(['./process', 'server', str(pid), sp2[2], sp2[3], sp2[4]], stdout=open('/dev/null', 'w'),
                    stderr=open('/dev/null', 'w'), preexec_fn=os.setsid)

            # sleep for a while to allow the process be ready
            time.sleep(3)
            # connect to the port of the pid
            handler = ClientHandler(pid, address, port, process)
            threads[pid] = handler
            handler.start()
        elif cmd == 'startClient':
            port = int(sp2[4])
            if pid not in started_processes:
                started_processes[pid] = True
            else:
                print "Process already started"
                exit()
            # start the process
            if debug:
                process = subprocess.Popen(['./process', 'client', str(pid), sp2[2], sp2[3], sp2[4]], preexec_fn=os.setsid)
            else:
                process = subprocess.Popen(['./process', 'client', str(pid), sp2[2], sp2[3], sp2[4]], stdout=open('/dev/null', 'w'),
                    stderr=open('/dev/null', 'w'), preexec_fn=os.setsid)

            # sleep for a while to allow the process be ready
            time.sleep(3)
            # connect to the port of the pid
            handler = ClientHandler(pid, address, port, process)
            threads[pid] = handler
            handler.start()
        elif cmd == 'put':
            send(pid, sp1[1], True)
        elif cmd == 'get':
            send(pid, sp1[1], True)
        elif cmd == 'block':
            # Id of message to block
            while wait_for_ack:
                time.sleep(0.1)
            pid = sp2[2]
            destId = sp2[3]
            block_lock.acquire()
            if pid not in blocked_ids:
                blocked_ids[pid] = {}
            blocked_ids[pid][destId] =True
            block_lock.release()
        elif cmd == 'unblock':
            while wait_for_ack:
                time.sleep(0.1)
            # Id of message to unblock
            mid = sp2[2]
            destId = sp2[3]
            block_lock.acquire()
            if mid in blocked_ids and destId in blocked_ids[mid]:
                del blocked_ids[mid][destId]
                # If have unblocked all the messages, delete
                if (not len(blocked_ids[mid])):
                    del blocked_ids[mid]
                if mid in blocked_messages:
                    if destId in blocked_messages[mid]:
                        blockedOp = blocked_messages[mid][destId]
                        forwardMsg(blockedOp)
                        del blocked_messages[mid][destId]
            block_lock.release()
        elif cmd == 'sleep':
            secs= sp2[2]
            time.sleep(float(secs))
        else:
            print "Unrecognised command"


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'debug':
        debug = True
    main()
