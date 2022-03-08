import socket
import sys

from _thread import *
import threading
class Run(object):
    def __init__(self, runID, min_budget, max_budget, worker):
        self.runId = runID
        self.min_budget = min_budget
        self.max_bugdet = max_budget
        self.worker = worker

    def add_worker(self, num=1):
        self.worker = self.worker + num

    def send_message(self):
        return self.runId + '!' +self.min_budget + '!' + self.max_bugdet + '!' + self.worker + '\n'


class Monitor(object):


    def __init__(self):
        self.run_dic = {}
        return

    def start(self, host='0.0.0.0', port=8080):
        server_socket = socket.socket()  # get instance
        # look closely. The bind() function takes tuple as argument
        server_socket.bind((host, port))  # bind host address and port together

        # configure how many client the server can listen simultaneously
        server_socket.listen(2)
        while True:
            conn, address = server_socket.accept()  # accept new connection
            print("Connection from: " + str(address))

            data = conn.recv(1024).decode()
            print(data)
            data = data.split('!')
            if len(data) != 5:
                break
            if data[0] == 'create':
                new_run = Run(runID=data[1], min_budget=data[2], max_budget=data[3], worker=data[4])
                self.run_dict[data[1]] = new_run
            elif data[0] == 'addworker':
                self.run_dict[data[1]].add_worker(num=int(data[4]))
            elif data[0] == 'show':
                for key in self.run_dict:
                    conn.send(self.run_dict[key].send_message())
            elif data[0] == 'remove':
                del self.run_dict[data[1]]



            conn.shutdown(socket.SHUT_WR)

            conn.close()


if __name__ == '__main__':
    monitor = Monitor()
    monitor.start()
