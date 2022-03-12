import os
import socket
import threading
import pickle

import hpbandster.core.nameserver as hpns
from importlib.machinery import SourceFileLoader
from hpbandster.optimizers import BOHB as BOHB

import multiprocessing


class Slave(object):
    def __init__(self, nic_name, port, monitor, monitor_port, share_dir):
        self.host = hpns.nic_name_to_host(nic_name)
        self.port = port
        self.monitor = monitor
        self.monitor_port = monitor_port
        self.share_dir = share_dir

    def run(self):
        client_socket = socket.socket()  # instantiate
        client_socket.connect((self.monitor, int(self.monitor_port)))  # connect to the monitor
        # TODO: dynamic avaliable position
        message = 'createserver!' + self.host + '!' + self.port + '!' + str(2*multiprocessing.cpu_count())
        client_socket.send(message.encode())
        client_socket.close()

        server_socket = socket.socket()  # get instance
        # look closely. The bind() function takes tuple as argument
        server_socket.bind((self.host, int(self.port)))  # bind host address and port together

        # configure how many client the server can listen simultaneously
        server_socket.listen(5)
        while True:
            conn, address = server_socket.accept()  # accept new connection
            print("Connection from: " + str(address))

            data = conn.recv(1024).decode()
            print(data)
            data = data.split('!')
            if data[0] == 'runnameserver':
                run_id = data[1]
                myworker = data[2]
                min_budget = float(data[3])
                max_budget = float(data[4])
                n_iterations = int(data[5])

                try:
                    thread = threading.Thread(target=self.BOHB_run, args=(run_id, myworker, min_budget, max_budget, n_iterations))
                    thread.start()
                except Exception as e:
                    print("Error: unable to start BOHB run thread: " + run_id)
                    print(e)

            elif data[0] == 'runworker':
                run_id = data[1]
                myworker = data[2]

                # self.worker_run(self, run_id, myworker)

                try:
                    thread = threading.Thread(target=self.worker_run, args=(run_id, myworker))
                    thread.start()
                except Exception as e:
                    print("Error: unable to start BOHB run thread: " + run_id)
                    print(e)

    def worker_run(self, run_id, myworker_name):
        foo = SourceFileLoader("worker", self.share_dir + myworker_name).load_module()
        w = foo.MyWorker(run_id=run_id, host=self.host)
        w.load_nameserver_credentials(working_directory=self.share_dir)
        print("worker run")
        w.run(background=True)
        print("worker run end")


    def BOHB_run(self, run_id, myworker_name,
                  min_budget,
                  max_budget,
                  n_iterations):
        # Start a nameserver:
        # We now start the nameserver with the host name from above and a random open port (by setting the port to 0)
        NS = hpns.NameServer(run_id=run_id, host=self.host, port=0, working_directory=self.share_dir)
        ns_host, ns_port = NS.start()

        foo = SourceFileLoader("MyWorke", self.share_dir + myworker_name).load_module()
        w = foo.MyWorker(run_id=run_id, host=self.host, nameserver=ns_host,
                         nameserver_port=ns_port)
        w.run(background=True)

        bohb = BOHB(configspace=foo.MyWorker.get_configspace(),
                    run_id=run_id,
                    host=self.host,
                    nameserver=ns_host,
                    nameserver_port=ns_port,
                    min_budget=min_budget, max_budget=max_budget,
                    monitor=self.monitor, monitor_port=int(self.monitor_port)
                    )
        res = bohb.run(n_iterations=n_iterations, min_n_workers=1)

        with open(os.path.join(self.share_dir, str(run_id) + '_results.pkl'), 'wb') as fh:
            pickle.dump(res, fh)

        # Step 4: Shutdown
        # After the optimizer run, we must shutdown the master and the nameserver.
        bohb.shutdown(shutdown_workers=True)
        NS.shutdown()

