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
import logging # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch

# import serialization logic
from CS6381_MW import discovery_pb2
#from CS6381_MW import topic_pb2  # you will need this eventually

# import any other packages you need.

class BrokerMW():
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger # internal logger for print statements
        self.req = None # a REQ socket binding to send requests to discovery service
        self.addr = None # our advertised IP address
        self.port = None # port num where we are going to broker
        self.pub = None # a PUB socket binding to publish to subscribers
        self.sub = None # a SUB socket binding to receive from publishers
        self.poller = None # a poller object to poll on the sockets
        self.upcall_obj = None # handle to appln obj to handle appln-specific data
        self.handle_events = True # in general we keep going thru the event loop
        self.zk = None # handle to zookeeper client
        self.discovery = None
        
    ########################################
    # configure/initialize
    ########################################
    
    def configure (self, args, topiclist):
        '''Initialize the object'''
        try:
            self.logger.info("BrokerMW::configure")
            # first retrieve our advertised ip addr and publication port num
            self.port = args.port
            self.addr = args.addr
            self.name = args.name
            
            # get ZMQ context
            self.logger.debug("BrokerMW::configure: obtain ZMQ context")
            context = zmq.Context()
            self.poller = zmq.Poller()
            
            # now aquire the sockets
            # REQ socket to talk to discovery service
            # PUB socket to publish to subscribers
            # SUB socket to receive from publishers
            self.logger.debug("BrokerMW::configure: obtain REQ, PUB, and SUB socket")
            self.req = context.socket(zmq.REQ)
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)
            

            # register our sockets iwth th poller
            self.logger.debug("BrokerMW::configure: register the REQ and SUB sockets with the poller")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)
            
            for topic in topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            
            
            
            # need to do both publisher and subscriber binding
            self.logger.debug("BrokerMW::configure: bind to the PUB")
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)
            
            self.logger.debug("BrokerMW::configure: creating ZK client")
            self.zk = KazooClient(hosts=args.zookeeper)
            self.zk.start()
            self.broker_leader(args.name)

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
                self.logger.info("SubscriberMW::set_req: disconnecting from {}".format(self.discovery))
                self.req.disconnect(self.discovery)
            self.req.connect(meta["rep_addr"])
            self.discovery = meta["rep_addr"]
            self.logger.debug("Successfully connected to leader")
                
        except Exception as e:
            raise e



    def setWatch(self):
        @self.zk.DataWatch("/broker")
        def watch_broker(data, stat):
            if data is None:
                self.logger.info("BrokerMW::watch_broker: broker node deleted, attempting to become leader")
                self.broker_leader(self.name)
                
        @self.zk.DataWatch("/leader")
        def watch_leader(data, stat):
            self.logger.info("BrokerMW::watch_leader: leader node changed")
            self.set_req()
        @self.zk.ChildrenWatch("/publisher")
        def watch_pubs(children):
            self.logger.info("BrokerMW::watch_pubs: publishers changed, re-subscribing")
            # self.set_req()
            publishers = []
            for child in children:
                path = "/publisher/" + child
                data, _ = self.zk.get(path)
                publishers.append(json.loads(data.decode("utf-8"))['id'])
            self.logger.info("BrokerMW::watch_pubs: {}".format(publishers))
            self.subscribe(publishers)

    def broker_leader(self, name):
        try:
            self.logger.info("BrokerMW::broker_leader")
            self.zk.start()
            self.logger.info("BrokerMW::broker_leader: connected to zookeeper")
            try:
                self.logger.info("BrokerMW::broker_leader: broker node does not exist, creating self")
                addr = {"id": name, "addr": self.addr, "port": self.port}
                data = json.dumps(addr)
                self.zk.create("/broker", value=data.encode('utf-8'), ephemeral=True, makepath=True)
            except NodeExistsError:
                self.logger.info("BrokerMW::broker_leader: broker node exists")
                

        except Exception as e:
            raise e
    
    
        
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

