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
        
    ########################################
    # configure/initialize
    ########################################
    
    def configure (self, args):
        '''Initialize the object'''
        try:
            self.logger.info("BrokerMW::configure")
            # first retrieve our advertised ip addr and publication port num
            self.port = args.port
            self.addr = args.addr
            
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
            
            
            # connect to the discovery service
            self.logger.debug("BrokerMW::configure: connect to the discovery service")
            connect_str = "tcp://" + args.discovery
            self.req.connect(connect_str)
            
            # need to do both publisher and subscriber binding
            self.logger.debug("BrokerMW::configure: bind to the PUB")
            bind_string = "tcp://*:" + str(self.port)
            self.pub.bind(bind_string)
            self.logger.info("BrokerMW::configure completed")
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
        
    def register(self, name, topiclist):
        try:
            self.logger.info("BrokerMW::register")
            
            self.logger.debug("BrokerMW::register: build the info")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            self.logger.debug("BrokerMW::register: build the request")
            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_BOTH
            
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = topiclist
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            
            disc_req.register_req.CopyFrom(register_req)
            self.logger.debug("BrokerMW::register: done building the request")

            self.logger.debug("BrokerMW::register: send the request")
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buffer: %s", buf2send)
            
            self.req.send(buf2send)
            
            self.logger.info("BrokerMW::register: done")
        except Exception as e:
            raise e
    def is_ready(self):
        try:
            self.logger.info ("BrokerMW::is_ready")

            # we do a similar kind of serialization as we did in the register
            # message but much simpler as the message format is very simple.
            # Then send the request to the discovery service
            
            # The following code shows serialization using the protobuf generated code.
            
            # first build a IsReady message
            self.logger.debug ("BrokerMW::is_ready - populate the nested IsReady msg")
            isready_req = discovery_pb2.IsReadyReq ()  # allocate 
            # actually, there is nothing inside that msg declaration.
            self.logger.debug ("BrokerMW::is_ready - done populating nested IsReady msg")

            # Build the outer layer Discovery Message
            self.logger.debug ("BrokerMW::is_ready - build the outer DiscoveryReq message")
            disc_req = discovery_pb2.DiscoveryReq ()
            disc_req.msg_type = discovery_pb2.TYPE_ISREADY
            # It was observed that we cannot directly assign the nested field here.
            # A way around is to use the CopyFrom method as shown
            disc_req.isready_req.CopyFrom (isready_req)
            self.logger.debug ("BrokerMW::is_ready - done building the outer message")
            
            # now let us stringify the buffer and print it. This is actually a sequence of bytes and not
            # a real string
            buf2send = disc_req.SerializeToString ()
            self.logger.debug ("Stringified serialized buf = {}".format (buf2send))

            # now send this to our discovery service
            self.logger.debug ("BrokerMW::is_ready - send stringified buffer to Discovery service")
            self.req.send (buf2send)  # we use the "send" method of ZMQ that sends the bytes
            
            # now go to our event loop to receive a response to this request
            self.logger.info ("BrokerMW::is_ready - request sent and now wait for reply")
            
        except Exception as e:
            raise e
        
    def lookup(self, topiclist):
        try:
            self.logger.info("BrokerMW::lookup")
            self.logger.debug("BrokerMW::lookup - setsockopt for each topic")
            for topic in topiclist:
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            disc_req = discovery_pb2.DiscoveryReq() #allocate
            disc_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS # we are looking up
            lookup_req = discovery_pb2.LookupPubByTopicReq() #allocate
            lookup_req.topiclist[:] = topiclist
            disc_req.lookup_req.CopyFrom(lookup_req)
            self.logger.info("BrokerMW::lookup - done building lookup request to discovery service")
            # stringify buffer and print it
            buf2send = disc_req.SerializeToString()
            self.logger.debug("Stringified serialized buf = {}".format(buf2send))
            
            # send to discovery service
            self.logger.debug("BrokerMW::lookup - send the request to discovery service")
            self.req.send(buf2send) # send the request to discovery service
            
            self.logger.info("BrokerMW::lookup - sent lookup message and now wait for reply")
        
        except Exception as e:
            raise e
    def subscribe(self, publist):
        try:
            self.logger.debug("BrokerMW::subscribe")
            for pub in publist:
                addr = "tcp://" + pub.addr + ":" + str(pub.port)
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

