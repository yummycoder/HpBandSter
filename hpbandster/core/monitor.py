import math
import socket
import sys
import uuid

from _thread import *
import threading
import time

# helper function: run job priodeicolly
from threading import Timer, Lock


class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False


class Run(object):
    def __init__(self, runID, min_budget, max_budget, worker, nameserver, nameserver_port, start_time):
        self.runId = runID
        self.min_budget = min_budget
        self.max_bugdet = max_budget
        self.worker = worker
        self.nameserver = nameserver
        self.nameserver_port = nameserver_port
        self.start_time = start_time
        self.check_time = start_time

    def add_worker(self, num=1):
        self.worker = self.worker + num

    def send_message(self):
        return self.runId + '!' + self.min_budget + '!' + self.max_bugdet + '!' + str(self.worker) + '\n'


class Server(object):
    def __init__(self, address='127.0.0.1', port=0, score=10):
        self.address = address
        self.port = port
        self.score = score
        self.worker_list = list()
        self.nameserver_list = list()
        self.score_lock = Lock()
        self.worker_lock = Lock()
        self.name_lock = Lock()

    def __lt__(self, other):
        return self.score < other.score

    def inc_score(self, num=1):
        self.score_lock.acquire()
        self.score = self.score + num
        self.score_lock.release()

    def dec_score(self, num=1):
        self.score_lock.acquire()
        self.score = self.score - num
        self.score_lock.release()

    def addworker(self, runId):
        self.worker_lock.acquire()
        self.worker_list.append(runId)
        self.worker_lock.release()
        self.dec_score()

    def addname(self, runId):
        self.name_lock.acquire()
        self.nameserver_list.append(runId)
        self.name_lock.release()
        self.dec_score()

    def delworker(self, runId):
        self.worker_lock.acquire()
        old_len = len(self.worker_list)
        self.worker_list = [value for value in self.worker_list if value != runId]
        new_len = len(self.worker_list)
        self.worker_lock.release()
        self.inc_score(new_len - old_len)

    def delname(self, runId):
        self.name_lock.acquire()
        old_len = len(self.nameserver_list)
        self.nameserver_list = [value for value in self.nameserver_list if value != runId]
        new_len = len(self.nameserver_list)
        self.name_lock.release()
        self.inc_score(new_len - old_len)


class Monitor(object):

    def __init__(self, host='0.0.0.0', port=8080, share_dir='./'):
        self.run_dict = {}
        self.server_list = []
        self.host = host
        self.port = port
        self.share_dir = share_dir
        self.total_score = 0
        self.myworker_dict = {}

    def check_worker(self):
        curr_time = time.time()
        for key in self.run_dict:
            print(str(key))
            new_worker=0
            if self.run_dict[key].worker < 8:
                new_worker = (2 * self.run_dict[key].worker) - self.run_dict[key].worker
                self.run_dict[key].check_time = curr_time
                print(new_worker)
            else:
                run_time = curr_time - self.run_dict[key].start_time
                new_worker = (8 + int(math.log(run_time, 10))) - self.run_dict[key].worker
                self.run_dict[key].check_time = curr_time
            print(self.run_dict[key].worker)
            print(new_worker)
            i = 0
            while i < new_worker:
                self.server_list.sort(reverse=True)
                if self.server_list[0].score > 0:
                    client_socket = socket.socket()  # instantiate
                    client_socket.connect((self.server_list[0].address, self.server_list[0].port))
                    message = 'runworker!' + key + '!' + self.myworker_dict[key]
                    client_socket.send(message.encode())
                    client_socket.close()
                    self.server_list[0].addworker(key)
                    i = i + 1
                else:
                    break

    def start(self):
        # run worker checker
        try:
            rt = RepeatedTimer(10, self.check_worker, )
        except Exception as e:
            print("Error: unable to start worker checker: ")
            print(e)
        try:
            self.start_monitor()
        finally:
            rt.stop()

    def start_monitor(self):
        server_socket = socket.socket()  # get instance
        # look closely. The bind() function takes tuple as argument
        server_socket.bind((self.host, self.port))  # bind host address and port together

        # configure how many client the server can listen simultaneously
        server_socket.listen(5)
        while True:
            conn, address = server_socket.accept()  # accept new connection
            print("Connection from: " + str(address))

            data = conn.recv(1024).decode()
            print(data)
            data = data.split('!')
            if data[0] == 'create':
                new_run = Run(runID=data[1], min_budget=data[2], max_budget=data[3], worker=int(data[4]),
                              nameserver=data[5], nameserver_port=data[6], start_time=time.time())
                self.run_dict[data[1]] = new_run
            elif data[0] == 'addworker':
                self.run_dict[data[1]].add_worker(num=int(data[4]))
            elif data[0] == 'show':
                for key in self.run_dict:
                    print(self.run_dict[key].send_message())
                    conn.send(self.run_dict[key].send_message().encode())
            elif data[0] == 'remove':
                curr_time = time.time()
                start_time = self.run_dict[data[1]].start_time
                print(data[1] + " finish runtime: " + str(curr_time - start_time))
                del self.run_dict[data[1]]
                for i in range(len(self.server_list)):
                    self.server_list[i].delworker(data[1])
                    self.server_list[i].delname(data[1])
            elif data[0] == 'createserver':
                server = Server(address=data[1], port=int(data[2]), score=int(data[3]))
                self.server_list.append(server)
                self.total_score = self.total_score + int(data[3])
            elif data[0] == 'assign':
                self.server_list.sort(reverse=True)
                if len(self.server_list) < 1:
                    message = 'NoServer!'
                    conn.send(message.encode())
                    continue
                if self.server_list[0].score > 0:
                    run_id = uuid.uuid1().int
                    myworker = data[1]
                    min_budget = data[2]
                    max_budget = data[3]
                    n_iterations = data[4]

                    name_server = self.server_list[0]
                    self.server_list[0].addname(run_id)

                    client_socket = socket.socket()  # instantiate
                    client_socket.connect((name_server.address, name_server.port))
                    message = 'runnameserver!' + str(run_id) + '!' + myworker + '!' + str(min_budget) + '!' + str(
                        max_budget) + '!' + str(n_iterations)
                    client_socket.send(message.encode())
                    client_socket.close()
                    conn.send(str(run_id).encode())

                    self.myworker_dict[str(run_id)] = myworker
                else:
                    message = 'wait!'
                    conn.send(message.encode())

            conn.shutdown(socket.SHUT_WR)

            conn.close()
