###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the broker middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the broker side of things.
#
# As mentioned earlier, a broker serves as a proxy and hence has both
# publisher and subscriber roles. So in addition to the REQ socket to talk to the
# Discovery service, it will have both PUB and SUB sockets as it must work on
# behalf of the real publishers and subscribers. So this will have the logic of
# both publisher and subscriber middleware.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch

# import serialization logic
from CS6381_MW import discovery_pb2
# from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.


class BrokerMW():
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.req = None  # a REQ socket binding to send requests to discovery service
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to broker
        self.pub = None  # a PUB socket binding to publish to subscribers
        self.sub = None  # a SUB socket binding to receive from publishers
        self.poller = None  # a poller object to poll on the sockets
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.zk = None  # handle to zookeeper client
        self.discovery = None
        self.groups = 0
        self.order = -1
        self.topiclist = None

    ########################################
    # configure/initialize
    ########################################

    def configure(self, args):
        '''Initialize the object'''
        try:
            self.logger.info("BrokerMW::configure")
            # first retrieve our advertised ip addr and publication port num
            self.port = args.port
            self.addr = args.addr
            self.name = args.name
            self.groups = args.groups
            # get ZMQ context
            self.logger.debug("BrokerMW::configure: obtain ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()

            # now aquire the sockets
            # REQ socket to talk to discovery service
            # PUB socket to publish to subscribers
            # SUB socket to receive from publishers
            self.logger.debug(
                "BrokerMW::configure: obtain REQ, PUB, and SUB socket")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)

            # register our sockets iwth th poller
            self.logger.debug(
                "BrokerMW::configure: register the REQ and SUB sockets with the poller")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            

            # need to do both publisher and subscriber binding
            self.logger.debug("BrokerMW::configure: bind to the PUB")
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)

            self.logger.debug("BrokerMW::configure: creating ZK client")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start()
            self.wait_group_creation()
            self.order = self.assignOrder(self.groups)
            self.group_no = self.order % self.groups
            self.broker_leader()
            self.set_req()

            self.logger.info("BrokerMW::configure completed")
        except Exception as e:
            raise e

    def set_req(self):
        try:
            while (self.zk.exists("/leader") == None):
                time.sleep(1)
            meta = json.loads(self.zk.get("/leader")[0].decode('utf-8'))
            if self.discovery != None:
                self.logger.info(
                    "SubscriberMW::set_req: disconnecting from {}".format(self.discovery))
                self.req.disconnect(self.discovery)
            self.req.connect(meta["rep_addr"])
            self.discovery = meta["rep_addr"]
            self.logger.debug("Successfully connected to leader")

        except Exception as e:
            raise e

    def setWatch(self):
        @self.zk.DataWatch("/broker/group/{}".format(self.order))
        def watch_broker(data, stat):
            if data is None:
                self.logger.info(
                    "BrokerMW::watch_broker: broker node deleted, attempting to become leader")
                self.broker_leader(self.name)

        @self.zk.DataWatch("/leader")
        def watch_leader(data, stat):
            self.logger.info("BrokerMW::watch_leader: leader node changed")
            self.set_req()

        @self.zk.ChildrenWatch("/publisher")
        def watch_pubs(children):
            # self.set_req()
            self.logger.info(
            "SubscriberMW::watch_pubs: publishers changed, re-subscribing")
            publishers = []
            for child in children:
                path = "/publisher/" + child
                data, _ = self.zk.get(path)
                publishers.append(json.loads(data.decode("utf-8")))
                self.logger.info("DiscoveryMW::watch_pubs: {}".format(publishers))
                self.upcall_obj.update_publishers_info(publishers)

    def assignOrder(self, number):
        # assign a number to itself from path /brokers/order
        try:
            number = 0

            self.zk.create("/brokers/order/{}".format(number),
                           value={self.name}, ephemeral=True, makepath=True)
            self.logger.info(
                "BrokerMW::assignOrder: assigned order {}".format(number))
            return number
        except NodeExistsError:
            return self.assignOrder(number + 1)
        except Exception as e:
            raise e

    def wait_group_creation(self, name):
        try:
            self.logger.info("BrokerMW::wait_group_creation")
            while (self.zk.exists("/brokers/group") == None):
                time.sleep(2)
            self.logger.info(
                "BrokerMW::wait_group_creation: broker group created")
            return
        except Exception as e:
            raise e

    def broker_leader(self):
        try:
            self.logger.info("BrokerMW::broker_leader")
            addr = {"id": self.name, "addr": self.addr, "port": self.port}
            data = json.dumps(addr)
            self.logger.info(
                "BrokerMW::broker_leader: attempting to create leader node for group {}".format(self.group_no))
            self.zk.create("/broker/group/{}/leader".format(self.group_no),
                           value=data, ephemeral=True, makepath=True)
            self.logger.info("BrokerMW::broker_leader: created leader node")
            return
        except NodeExistsError:
            self.logger.info(
                "BrokerMW::broker_leader: leader node already exists")
        except Exception as e:
            raise e

 
    def get_topics(self):
        while (self.zk.exists("/broker/group/{}".format(self.group_no)) != None):
            time.sleep(1)
        data, _ = self.zk.get("/broker/group/{}".format(self.group_no))
        topics = json.loads(data.decode("utf-8"))['topics']
        self.topiclist = topics
        for topic in topics:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
        return topics
         
    ########################################
    # run event loop 
    ########################################
    def event_loop(self, timeout=None):
        try:
            self.logger.info("BrokerMW::event_loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout))
                
                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    timeout = self.handle_reply()
                elif self.sub in events:
                    timeout = self.recv_data()
                else:
                    raise Exception("BrokerMW::event_loop: unknown event after poll")
            self.logger.info("BrokerMW::event_loop: event loop terminated")
            
        except Exception as e:
            raise e
    
    def handle_reply(self):
        try:
            self.logger.debug("BrokerMW::handle_reply")
            # receive the reply
            bytesRcvd = self.req.recv()
            
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            
            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                timeout = self.upcall_obj.isready_response(disc_resp.isready_resp)
        except Exception as e:
            raise e
        
    
    def subscribe(self, publist):
        try:
            self.logger.info("BrokerMW::subscribe")
            for pub in publist:
                addr = "tcp://" + pub['addr'] + ":" + str(pub['port'])
                self.logger.info("BrokerMW::subscribe: subscribing to {}".format(addr))
                self.sub.connect(addr)
        except Exception as e:
            raise e
        
    def recv_data(self):
        try:
            msg = self.sub.recv_multipart()
            self.logger.debug("BrokerMW::recv_data - received data: %s", msg)
            self.pub.send_multipart(msg)
            
            self.logger.debug("BrokerMW::recv_data - done disseminating publisher data")
        except Exception as e:
            raise e
    ########################################
    # set upcall handle
    #
    # here we save a pointer (handle) to the application object
    ########################################
    def set_upcall_handle (self, upcall_obj):
        ''' set upcall handle '''
        self.upcall_obj = upcall_obj

    ########################################
    # disable event loop
    #
    # here we just make the variable go false so that when the event loop
    # is running, the while condition will fail and the event loop will terminate.
    ########################################
    def disable_event_loop (self):
        ''' disable event loop '''
        self.handle_events = False

