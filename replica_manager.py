import math, time, datetime, random, shutil, threading, subprocess, docker
from threading import Lock
from docker import types
from pprint import pprint

class Replica_Manager:
    SERVICE_NAME = ""
    SERVICE = None
    lock = Lock()
    replicas = {}
    index = 0

    def __init__(self, image, server, port, SRN):
        print("--------------------------------------------------------------")
        print("Starting the service ")
        self.IMAGE = image
        self.SERVER = server
        self.PORT = port
        for ch in image:
            self.SERVICE_NAME += "_" if ch == "/" else ch
        self.SERVICE_NAME += "_"
        self.SERVICE_NAME += str(random.randrange(1000,9999))
        self.start_service(SRN)
        #subprocess.run(["docker", "service", "create" , "--name", self.SERVICE_NAME, image, "--server", server, "--port", str(port)], stdout=subprocess.PIPE)

    def start_service(self, SRN):
        self.docker_client = docker.from_env()
        env = ["SERVER=" + self.SERVER, "PORT=" + str(self.PORT)]
        mode = types.ServiceMode(mode='replicated', replicas= SRN)
        self.SERVICE = self.docker_client.services.create(self.IMAGE, command=None, env= env, mode= mode, name=self.SERVICE_NAME)

    def stop_service(self):
        self.SERVICE.remove()

    def __str__(self):
        text = ""
        return text

    def register(self, client_address):
        self.lock.acquire()
        replica_id = -1
        try:
            # only one thread can execute code there
            replica_id = "REPLICA_" + str(self.index)
            self.index += 1
            now = time.time()
            replica = {'ip': client_address[0], 'port': client_address[1], 'registered_at':now, 'last_still_alive_at': now}
            self.replicas[replica_id] = replica
        finally:
            self.lock.release() #release lock
        print(self.replicas)
        return replica_id

    def still_alive(self,replica_id, data):
        # only one thread can execute code there
        now = time.time()
        self.replicas[replica_id]['last_still_alive_at'] = now
        #print(self.replicas)
        return now

    def get_replicas(self):
        return self.replicas

    def is_replica_alive(self, replica_detail):
        now = time.time()
        if (now - float(replica_detail['last_still_alive_at']) > 12):
            return False
        else:
            return True

    def replica_count(self):
        count = 0
        for replica_id, replica_detail in self.replicas.items():
            if self.is_replica_alive(replica_detail):
                count += 1
        return count

    def get_service_data(self):
        return self.docker_client.services.get(self.SERVICE_NAME)

    def docker_replica_count(self):
        return int(self.get_service_data().attrs['Spec']['Mode']['Replicated']['Replicas'])

    def add_replicas(self, count):
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print(self.SERVICE.attrs['Spec']['Mode'])
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        subprocess.run(["docker", "service", "scale" , self.SERVICE_NAME + "=" + str(self.replica_count() + count)], stdout=subprocess.PIPE)

