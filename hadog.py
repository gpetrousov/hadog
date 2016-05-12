"""
Ioannis Petrousov
petrousov@gmail.com
07-Apr-16

"""

import marathon_api
import requests
import json
import time
import os
import sys
import signal
import filecmp
import shutil
from subprocess import call
from os import system

#docker socket runs on
DOCKER_SOCKET_PORT = '4243'

MARATHON_URL = 'http://83.212.124.105:8080'
INTERVAL = 10

#=============-------------
#=============-------------
#=============-------------

class MarathonApp:
    """TODO : Insert docstring here"""
    def __init__(self, app_id, priority = None, cpu = None, ram = None, rps = None):
        self.marathon_app_id = app_id
        self.ports = marathon_api.get_ports(MARATHON_URL, self.marathon_app_id) #these are for frontends
        self.front_back_names = [] #common names for frontends and backends
        self.create_front_back_names()
        self.hosts = marathon_api.get_hosts(MARATHON_URL, self.marathon_app_id)
        self.hosts_ports = marathon_api.get_hosts_ports(MARATHON_URL, self.marathon_app_id)

        #self.instances = marathon_api.get_nof_instances(MARATHON_URL, self.marathon_app_id)
        #self.priority = priority #which resource has priority in usage
        #self.cpu = cpu #limit for the cpu
        #self.ram = ram #limit for the ram
        #self.rps = rps #limit for the rps
        
    def get_avg_ram_usage(self):
        """Return average percentage of RAM usage of all containers on all hosts."""
        total_ram_used = 0
        for h in self.hosts: #for each host
            slave_url = 'tcp://' + h + ':' + DOCKER_SOCKET_PORT
            slave = Client(slave_url)
            all_containers = slave.containers()
            for each_container in all_containers: #for each container
                if self.service_name in each_container['Image']:
                    container_id = each_container['Id']
                    all_stats_bytes = next(slave.stats(container_id)) #bytes TODO take stats with requests
                    all_stats_str = all_stats_bytes.decode("utf-8") #str
                    all_stats_json = json.loads(all_stats_str) #json-dict
                    total_ram_used += all_stats_json['memory_stats']['usage']
        ram_limit = all_stats_json['memory_stats']['limit']
        print "CUrrent number of instances ", self.instances
        return ((float(total_ram_used / self.instances)) * 100) / ram_limit

    def get_avg_cpu_usage(self):
        """Return average percentage of CPU usage of all containers on all hosts"""
        total_cpu_used = 0
        for h in self.hosts: #for each host
            slave_url = 'tcp://' + h + ':' + DOCKER_SOCKET_PORT
            slave = Client(slave_url)
            all_containers = slave.containers()
            for each_container in all_containers: #for each container
                if self.service_name in each_container['Image']:
                    container_id = each_container['Id']
                    all_stats_bytes = next(slave.stats(container_id)) #bytes TODO take stats with requests
                    all_stats_str = all_stats_bytes.decode("utf-8") #str
                    all_stats_json = json.loads(all_stats_str) #json-dict
                    cpu_time1 = all_stats_json[u'cpu_stats'][u'cpu_usage'][u'total_usage']
                    timestamp1 = time.time()
                    time.sleep(1)
                    all_stats_bytes = next(slave.stats(container_id)) #bytes TODO take stats with requests
                    all_stats_str = all_stats_bytes.decode("utf-8") #str
                    all_stats_json = json.loads(all_stats_str) #json-dict
                    cpu_time2 = all_stats_json[u'cpu_stats'][u'cpu_usage'][u'total_usage']
                    timestamp2 = time.time()

                    total_cpu_used += (float(cpu_time2 - cpu_time1) / 1000000000) / (timestamp2 - timestamp1) * 100
        return float(total_cpu_used / self.instances)

    def create_front_back_names(self):
        """ <app_id>_<port> """
        for each_port in self.ports:
            self.front_back_names.append(self.marathon_app_id + "_" + str(each_port))
        return

#=============-------------
#=============-------------
#=============-------------

def insert_default_config(fd):
    """ Write standard configuration for haproxy """
    stdconf = """global
        daemon
        log 127.0.0.1 local2
        stats socket /var/run/haproxy.sock mode 600 level admin
        nbproc  1

    defaults
        log               global
        retries           0
        option            redispatch
        maxconn           200000
        timeout connect   10000 #time to wait for a connection attempt to a server to succeed (backend)
        timeout queue     11000 #time to wait in the queue for a connection slot to be free
        option http-keep-alive #Enable or disable HTTP keep-alive from client to server
        timeout http-keep-alive 10000 #the maximum allowed time to wait for a new HTTP request to appear

        #Errors detected and returned by HAProxy
        errorfile 400 /etc/haproxy/errors/400.http
        errorfile 403 /etc/haproxy/errors/403.http
        errorfile 408 /etc/haproxy/errors/408.http
        errorfile 500 /etc/haproxy/errors/500.http
        errorfile 502 /etc/haproxy/errors/502.http
        errorfile 503 /etc/haproxy/errors/503.http
        errorfile 504 /etc/haproxy/errors/504.http

    listen stats
        bind 0.0.0.0:9090
        balance
        stats show-legends #Enable reporting additional information on the statistics page
        stats refresh 1s
        mode http
        stats enable
        stats realm Haproxy\ Statistics
        stats uri /
        stats auth 123:123\n"""

    fd.write(stdconf)
    return 0

#=============-------------
#=============-------------
#=============-------------


def open_file_write(path):
    fd = open(path, 'w')
    return fd

#=============-------------
#=============-------------
#=============-------------

def open_file_append(path):
    fd = open(path, 'a')
    return fd

#=============-------------
#=============-------------
#=============-------------

def create_marathon_objects():
    """Read resource_definitions.json and create MarathonApp class objects."""

    #fetch data from marathon
    headers = {'Content-Type': 'application/json'}
    r = requests.get('http://83.212.124.105:8080/v2/apps', headers = headers)
    jdata = r.json()['apps']

    marathon_app_objects = []
    #create data
    for app in jdata:
        marathon_app_objects.append(MarathonApp(str(app['id']).replace('/', '')))

    return marathon_app_objects

#=============-------------
#=============-------------
#=============-------------

def create_all_frontends_backends_for_haproxy(app_object):
    """ Create frontend backend pairs given marathon_app_object """
    hastring = ""
    for i in xrange(len(app_object.front_back_names)):
        # i is frontend index
        hastring += "\nfrontend " + app_object.front_back_names[i]
        hastring += "\nbind *:" + str(app_object.ports[i])
        hastring += "\nmode tcp"
        hastring += "\nuse_backend " + app_object.front_back_names[i]

        hastring += "\n\nbackend " + app_object.front_back_names[i]
        hastring += "\nbalance leastconn"
        hastring += "\nmode tcp"
        if 'marathon_app_10000' in app_object.front_back_names[i]:
            #insert http check to string
            hastring += "\noption httpchk GET /health/check"
            hastring += "\nhttp-check disable-on-404"
        for j in xrange(len(app_object.hosts_ports)):
            # j is port group index
            if 'marathon_app_10000' in app_object.front_back_names[i]:
                #insert http check to string
                hastring += "\n  server " + app_object.hosts[j].replace('.', '_') + "_" + str(app_object.hosts_ports[j][i])
                hastring += " " + app_object.hosts[j] + ":" + str(app_object.hosts_ports[j][i])
                hastring += "check port " + str(app_object.hosts_ports[j][i]) + " inter 10s fall 1 rise 2"
            else:
                #no checks here
                hastring += "\n  server " + app_object.hosts[j].replace('.', '_') + "_" + str(app_object.hosts_ports[j][i])
                hastring += " " + app_object.hosts[j] + ":" + str(app_object.hosts_ports[j][i])
        hastring += "\n"
    return hastring

#=============-------------
#=============-------------
#=============-------------

def main():

    def handler(*args):
        """Signal handler for CTRL + ^c"""
        print "Recieved SIGINT\nTerminating\n################\nBye Bye!\n################"
        sys.exit(0)

    signal.signal(signal.SIGINT,handler)
    marathon_app_objects = create_marathon_objects()
    tmp_haproxy_fd = open_file_write('/tmp/haproxy.cfg')
    insert_default_config(tmp_haproxy_fd)
    tmp_haproxy_fd.close()

    haproxy_string = ""
    for each_app in marathon_app_objects:
        haproxy_string += create_all_frontends_backends_for_haproxy(each_app)
    tmp_haproxy_fd = open_file_append('/tmp/haproxy.cfg')
    tmp_haproxy_fd.write(haproxy_string)
    tmp_haproxy_fd.close()

    #compare two files
    if filecmp.cmp('/tmp/haproxy.cfg', '/etc/haproxy/haproxy.cfg') == False:
        pass
        #files don't match
        shutil.copyfile('/tmp/haproxy.cfg', '/etc/haproxy/haproxy.cfg')
        #call(["haproxy -D -f /etc/haproxy/haproxy.cfg -p /var/run/haproxy.pid -sf $(cat /var/run/haproxy.pid)"])
        os.system("/usr/sbin/haproxy -D -f /etc/haproxy/haproxy.cfg -p  /var/run/haproxy.pid -sf $(cat /var/run/haproxy.pid)")

    return


#=============-------------
#=============-------------
#=============-------------

if __name__ == '__main__':
    main()
