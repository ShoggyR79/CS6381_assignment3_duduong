###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student. Please see the
# PublisherMW.py file as to how the middleware side of things are constructed
# and accordingly design things for the subscriber side of things.
#
# Remember that the subscriber middleware does not do anything on its own.
# It must be invoked by the application level logic. This middleware object maintains
# the ZMQ sockets and knows how to talk to Discovery service, etc.
#
# Here is what this middleware should do
# (1) it must maintain the ZMQ sockets, one in the REQ role to talk to the Discovery service
# and one in the SUB role to receive topic data
# (2) It must, on behalf of the application logic, register the subscriber application with the
# discovery service. To that end, it must use the protobuf-generated serialization code to
# send the appropriate message with the contents to the discovery service.
# (3) On behalf of the subscriber appln, it must use the ZMQ setsockopt method to subscribe to all the
# user-supplied topics of interest.
# (4) Since it is a receiver, the middleware object will maintain a poller and even loop waiting for some
# subscription to show up (or response from Discovery service).
# (5) On receipt of a subscription, determine which topic it is and let the application level
# handle the incoming data. To that end, you may need to make an upcall to the application-level
# object.
#

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import timeit  # for latency measurement
import signal  # for signal handling
import csv  # for csv file writing
import json

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch
# import serialization logic
from CS6381_MW import discovery_pb2


class SubscriberMW():
    def __init__(self, logger):
        # save the logger
        self.logger = logger
        self.sub = None
        self.req = None
        self.poller = None
        # self.addr = None
        # self.port = None
        self.upcall_obj = None
        self.handle_events = True
        self.latency = None
        # self.start_time = None
        # self.reset_time = None
        self.filename = None
        self.zk = None
        self.discovery = None

        self.dissemination_method = None
    # configure/initialize

    def configure(self, args, dissemination_method, topiclist):
        try:

            self.logger.debug("SubscriberMW: configure")
            self.filename = args.filename
            self.dissemination_method = dissemination_method
            # # First retrieve our advertised IP addr and the subscriber port num
            # self.port = args.port
            # self.addr = args.addr
            self.logger.debug("SubscriberMW::configure: creating ZK client")
            self.zk = KazooClient(hosts=args.zookeeper)

            # Next get the ZMQ context
            self.logger.debug("SubscriberMW: configure: obtain ZMQ context")
            context = zmq.Context()  # returns a singleton object
            # get the ZMQ poller object
            self.logger.debug("SubscriberMW: configure: obtain ZMQ poller")
            self.poller = zmq.Poller()

            # acqurire the req and sub sockets
            self.logger.debug(
                "SubscriberMW: configure: obtain REQ and sub sockets")
            self.req = context.socket(zmq.REQ)
            self.sub = context.socket(zmq.SUB)

            
            # get the ZMQ poller object
            self.logger.debug("SubscriberMW: configure: obtain ZMQ poller")
            self.poller.register(self.req, zmq.POLLIN)
            self.poller.register(self.sub, zmq.POLLIN)

            for topic in topiclist:
                self.logger.debug("SubscriberMW: configure: subscribe to topic {}".format(topic))
                self.sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            # Now connect ourselves to the discovery service. Recall that the IP/port were
            # supplied in our argument parsing.
            self.logger.debug(
                "SubscriberMW: configure: connect to discovery service")
            self.zk.start()

            self.set_req()

            self.logger.info("SubscriberMW::configure completed")

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
        @self.zk.DataWatch("/leader")
        def watch_leader(data, stat):
            self.logger.info("SubscriberMW::watch_leader: leader node changed")
            self.set_req()

            return

        @self.zk.DataWatch("/broker")
        def watch_broker(data, stat):
            self.logger.info("SubscriberMW::watch_broker: broker node changed - {} and {}".format(self.dissemination_method, data))
            if self.dissemination_method == "Broker" and data is not None:
                meta = json.loads(self.zk.get("/broker")[0].decode('utf-8'))
                self.logger.info("SubscriberMW::watch_broker: broker changed, re-subscribing to {}".format(meta["id"]))
                self.subscribe([meta])
        if self.dissemination_method == "Direct":
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

    def event_loop(self, timeout=None):
        try:
            self.logger.debug("SubscriberMW: event_loop - run the event loop")
            while self.handle_events:

                events = dict(self.poller.poll(timeout=(timeout)))
                # check if a timeout occurred.
                if not events:
                    # make an upcall ot the generic "invoke_operations"
                    timeout = self.upcall_obj.invoke_operation()
                elif self.req in events:
                    # handle the reply from remote entity and return the result
                    timeout = self.handle_reply()
                elif self.sub in events:
                    timeout = self.recv_data()  # handle the data
                else:
                    raise Exception("Unknown event after poll")
            self.logger.info("SubscriberMW: event_loop: out of the event loop")
        except KeyboardInterrupt:
            self.logger.info("SubscriberMW: event_loop: Ctrl-C received")
            self.disable_event_loop()
        except Exception as e:
            raise e

    def handle_reply(self):
        try:
            self.logger.debug("SubscriberMW: handle_reply")

            # receive the reply
            bytesRcvd = self.req.recv()

            # use protobuf to deserialize the bytes
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)
            # depending on the message type, the remaining
            # contents of the msg will differ

            if (disc_resp.msg_type == discovery_pb2.TYPE_REGISTER):
                # let the appln level object decide what to do
                timeout = self.upcall_obj.register_response(
                    disc_resp.register_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_ISREADY):
                # let the appln level object decide what to do
                timeout = self.upcall_obj.isready_response(
                    disc_resp.isready_resp)
            elif (disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_response(
                    disc_resp.lookup_resp.publist)
            else:  # anything else is unrecognizable by this object
                # raise an exception here
                raise Exception("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    

    def subscribe(self, publist):
        try:
            with open(self.filename, "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Latency"])

            self.logger.debug("SubscriberMW::subscribe")
            self.start_time = timeit.default_timer()
            self.logger.info("SubscriberMW::subscribe: {}".format(publist))
            for pub in publist:
                addr = "tcp://10.0.0.7:5577"
                self.logger.info("SubscriberMW::subscribe: connecting to {}".format(addr))
                self.sub.connect(addr)

        except Exception as e:
            raise e

    def recv_data(self):
        try:
            data = self.sub.recv_multipart()
            message = discovery_pb2.Publication()
            message.ParseFromString(data[1])
            timestamp = message.timestamp
            topic = message.topic
            data = message.data
            recv_time = time.monotonic()
            latency = recv_time - message.timestamp
            data_point = ((recv_time - self.start_time), latency)
            self.write_csv(self.filename, data_point)
            self.logger.debug(
                "SubscriberMW::recv_data, value = {}: {}- {}".format(timestamp, topic, data))
            # print("Subscriber::recv_data, value = {}: {}- {}".format(timestamp, topic, data))
            self.logger.debug("Time Received: {} \nTime Sent: {}\nLatency = {}".format(recv_time, message.timestamp, latency))
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        '''set the upcall object'''
        try:
            self.logger.debug("SubscriberMW: set_upcall_handle")
            self.upcall_obj = upcall_obj
        except Exception as e:
            raise e

    def disable_event_loop(self):
        '''disable the event loop'''
        self.handle_events = False
        # print("writing to file")

        #     for x,y in self.latency:
        #         writer.writerow([x,y])

    def write_csv(self, file_name, data):
        with open(file_name, "a", newline='') as f:
            writer = csv.writer(f)
            # print(data)
            x, y = data
            writer.writerow([x, y])

    # def interrupt_handler(self, signal, frame):
    #     print('Terminating... and printing to file')
    #     self.disable_event_loop()
    #     sys.exit(0)

    # signal.signal(signal.SIGINT, interrupt_handler)
