###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the discovery middleware code
#
# Created: Spring 2023
#
###############################################

# Designing the logic is left as an exercise for the student.
#
# The discovery service is a server. So at the middleware level, we will maintain
# a REP socket binding it to the port on which we expect to receive requests.
#
# There will be a forever event loop waiting for requests. Each request will be parsed
# and the application logic asked to handle the request. To that end, an upcall will need
# to be made to the application logic.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import logging  # for logging. Use it in place of print statements.
import zmq  # ZMQ sockets
import json  # for json serialization/deserialization

# import serialization logic
from CS6381_MW import discovery_pb2

# import the Kazoo package
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.recipe.election import Election
from kazoo.recipe.watchers import DataWatch


##################################
#       Discovery Middleware class
##################################


class DiscoveryMW():
    ########################################
    # constructor
    ########################################
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.rep = None  # a REP socket binding to receive requests
        self.addr = None  # our advertised IP address
        self.port = None  # port num where we are going to publish our topics
        self.upcall_obj = None  # handle to appln obj to handle appln-specific data
        self.handle_events = True  # in general we keep going thru the event loop
        self.zk = None  # handle to the zookeeper client
        self.pub = None  # handle publishing from leader to replicas
        self.sub = None  # handle receiving info from leader

    ########################################
    # configure/initialize
    ########################################
    def configure(self, args):
        '''Initialize the object'''
        try:
            self.logger.info("DiscoveryMW::configure")

            # retrieve our advertised ip addr and publication port num
            self.port = args.port
            self.addr = args.addr

            # obtain the ZMQ context
            self.logger.debug("DiscoveryMW::configure: obtain ZMQ context")
            context = zmq.Context()

            # TODO: POLLER HERE
            self.logger.debug("DiscoveryMW::configure: create ZMQ POLLER")
            self.poller = zmq.Poller()

            self.logger.debug("DiscoveryMW::configure: create ZMQ REP socket")
            self.rep = context.socket(zmq.REP)

            self.logger.debug("DiscoveryMW::configure: register poller")
            self.poller.register(self.rep, zmq.POLLIN)

            # bind socket to the address
            self.logger.debug(
                "DiscoveryMW::configure: bind REP socket to address")
            self.bind_string = "tcp://*:" + str(self.port)
            self.rep.bind(self.bind_string)

            self.logger.debug(
                "DiscoveryMW::configure: create ZMQ PUB and SUB socket")
            self.pub = context.socket(zmq.PUB)
            self.sub = context.socket(zmq.SUB)
            self.poller.register(self.sub, zmq.POLLIN)

            self.bind_string = "tcp://*:" + str(self.port + 1)
            self.pub.bind(self.bind_string)
            self.logger.debug("DiscoveryMW::configure: create ZK client")
            self.zk = KazooClient(hosts=args.zookeeper)
            # establishing quorum
            self.quorum = args.quorum
            self.ensure_quorum(args.name)
            self.logger.debug(
                "DiscoveryMW::configure: ZK client state = {}".format(self.zk.state))
            # if /leader znode gets deleted, we try to create new leader

            @self.zk.DataWatch("/leader")
            def watch_leader(data, stat):
                if data is None:
                    self.logger.info(
                        "DiscoveryMW::watch_leader: leader deleted, trying to create new leader")
                    self.create_leader(args.name)

            @self.zk.DataWatch("/broker")
            def watch_broker(data, stat):
                if data is not None:
                    self.logger.info(
                        "DiscoveryMW::watch_broker: broker changed, updating broker info")
                    broker_info = json.loads(data.decode("utf-8"))
                    self.upcall_obj.update_broker_info(broker_info)
            # creating /publisher znode if it doesn't exist
            try:
                self.zk.create("/publisher", makepath=True)
            except NodeExistsError:
                pass
            @self.zk.ChildrenWatch("/publisher")
            def watch_pubs(children):
                self.logger.info(
                    "DiscoveryMW::watch_pubs: publisher changed, updating publisher info")
                publishers = []
                for child in children:
                    path = "/publisher/" + child
                    data, _ = self.zk.get(path)
                    publishers.append(json.loads(data.decode("utf-8")))
                self.logger.info("DiscoveryMW::watch_pubs: {}".format(publishers))
                self.upcall_obj.update_publisher_info(publishers)
                return
            self.logger.info("DiscoveryMW::configure completed")

        except Exception as e:
            raise e
    ###############################################
    # create leader else wait until leader is done
    ###############################################
    def ensure_quorum(self, name):
        try:
            self.logger.info("DiscoveryMW::ensure_quorum")
            self.zk.start()
            self.logger.info(
                "DiscoveryMW::ensure_quorum: ZK client state = {}".format(self.zk.state))
            # create a znode under /discovery to count the number of quorum in while loop
            self.zk.create("/discovery/" + name, ephemeral=True, makepath=True)
            while (len(self.zk.get_children("/discovery")) < self.quorum):
                self.logger.info(
                    "DiscoveryMW::ensure_quorum: quorum_size not met, waiting for 2s")
                time.sleep(2)
            self.logger.info(
                "DiscoveryMW::ensure_quorum: quorum_size met, creating leader")
            self.create_leader(name)
        except Exception as e:
            raise e
        
    def create_leader(self, name):
        try:
            try:
                self.logger.info(
                    "DiscoveryMW::create_leader: attempting to create new ephemeral leader")
                rep_addr = "tcp://" + self.addr + ":" + str(self.port)
                pub_addr = "tcp://" + self.addr + ":" + str(self.port + 1)
                meta_str = json.dumps(
                    {"name": name, "rep_addr": rep_addr, "pub_addr": pub_addr})
                self.zk.create(
                    "/leader", value=meta_str.encode("utf-8"), ephemeral=True, makepath=True)
                self.logger.info("DiscoveryMW::create_leader: leader created")
            except NodeExistsError:
                self.logger.info(
                    "DiscoveryMW::create_leader: leader already exists, connecting to leader through SUB socket")
                meta = json.loads(self.zk.get("/leader")[0].decode("utf-8"))
                self.logger.info(
                    "DiscoveryMW::create_leader: leader address = {}".format(meta["pub_addr"]))
                self.sub.connect(meta["pub_addr"])
                self.sub.setsockopt_string(zmq.SUBSCRIBE, "backup")
                return
            
        except Exception as e:
            raise e

    #################################################################
    # run the event loop where we expect to receive a reply to a sent request
    #################################################################

    def event_loop(self, timeout=None):
        try:
            self.logger.info("DiscoveryMW::event_loop: start")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))

                if not events:
                    timeout = self.upcall_obj.invoke_operation()
                elif self.rep in events:
                    timeout = self.handle_request()
                elif self.sub in events:
                    timeout = self.recv_from_leader()
                else:
                    raise Exception("Unknown event after poll")
            self.logger.info("DiscoveryMW::event_loop out of the event loop")
        except Exception as e:
            raise e

    # recv states update from leader
    def recv_from_leader(self):
        try:
            data_recv = self.sub.recv_multipart()
            # [ttp, pti, count_pub, count_sub, state]
            ttp = json.loads(data_recv[0].decode("utf-8"))
            pti = json.loads(data_recv[1].decode("utf-8"))

            self.upcall_obj.update_state(
                ttp, pti, data_recv[2], data_recv[3])
        except Exception as e:
            raise e
    # handle the poller request:

    def handle_request(self):
        try:
            self.logger.info("DiscoveryMW::handle_request:")
            # receive message
            bytesRcvd = self.rep.recv()

            # parse the request
            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.ParseFromString(bytesRcvd)

            # we have a request. Now we need to handle it. For that we need
            # to call the application logic. So we need to upcall to the
            # application logic. We will pass the request
            if (disc_req.msg_type == discovery_pb2.TYPE_REGISTER):
                # make an upcall to the application logic
                timeout = self.upcall_obj.handle_register(
                    disc_req.register_req)
            elif (disc_req.msg_type == discovery_pb2.TYPE_ISREADY):
                # the is ready message should be empty
                timeout = self.upcall_obj.isready_request()
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC):
                timeout = self.upcall_obj.lookup_pub_by_topic_request(
                    disc_req.lookup_req.topiclist, True)
            elif (disc_req.msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS):
                timeout = self.upcall_obj.lookup_all_pubs_request(
                    disc_req.lookup_req.topiclist, False)
            else:
                raise ValueError(
                    "DiscoveryMW::event_loop: unknown message type")
            return timeout
        except Exception as e:
            raise e

    # is_ready_reply

    def is_ready_reply(self, status):
        try:
            self.logger.debug("DiscoveryMW::is_ready_reply")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_ISREADY
            isready_resp = discovery_pb2.IsReadyResp()
            isready_resp.status = status
            disc_rep.isready_resp.CopyFrom(isready_resp)

            self.logger.debug(
                "DiscoveryMW::is_ready_reply: done building reply")

            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug(
                "Stringified serialized buffer = {}".format(buf2send))

            # send this to the client
            self.logger.debug("DiscoveryMW::is_ready_reply: send reply")
            self.rep.send(buf2send)

            self.logger.info(
                "DiscoveryMW::is_ready_reply: done replying to client is_ready request")
        except Exception as e:
            raise e

    def register_reply(self, status):
        try:
            self.logger.debug("DiscoveryMW::register_reply")
            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_REGISTER
            register_resp = discovery_pb2.RegisterResp()
            register_resp.status = status
            disc_rep.register_resp.CopyFrom(register_resp)

            self.logger.debug(
                "DiscoveryMW::register_reply: done building reply")

            # stringify the buffer
            buf2send = disc_rep.SerializeToString()
            self.logger.debug(
                "Stringified serialized buffer = {}".format(buf2send))

            # send this to the client
            self.logger.debug("DiscoveryMW::register_reply: send reply")
            self.rep.send(buf2send)

            self.logger.info(
                "DiscoveryMW::register_reply: done replying to client register request")
        except Exception as e:
            raise e
    # set upcall handle

    def lookup_pub_by_topic_reply(self, publist):
        try:
            self.logger.debug("DiscoveryMW::lookup_reply")

            disc_rep = discovery_pb2.DiscoveryResp()
            disc_rep.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
            lookup_resp = discovery_pb2.LookupPubByTopicResp()
            pub_info = []
            for pub in publist:
                info = discovery_pb2.RegistrantInfo()
                info.id = pub.id
                info.addr = pub.addr
                info.port = pub.port
                pub_info.append(info)

            lookup_resp.publist.extend(publist)
            disc_rep.lookup_resp.CopyFrom(lookup_resp)

            self.logger.debug("DiscoveryMW::lookup_reply: done building reply")

            buf2send = disc_rep.SerializeToString()
            self.logger.debug(
                "Stringified serialized buffer = {}".format(buf2send))

            # send to client
            self.logger.debug("DiscoveryMW::lookup_reply: send reply")
            self.rep.send(buf2send)

            self.logger.info(
                "DiscoveryMW::lookup_reply: done replying to client lookup request")
        except Exception as e:
            raise e

    def set_upcall_handle(self, upcall_obj):
        self.upcall_obj = upcall_obj

    # disable event loop
    def disable_event_loop(self):
        self.handle_events = False

    def send_state_to_replica(self, topics_to_publisher, publisher_to_ip, count_pub, count_sub, state):
        ttp = json.dumps(topics_to_publisher)
        pti = json.dumps(publisher_to_ip)
        self.logger.info("DiscoveryMW::send_state_to_replica: sending state to replica")
        self.pub.send_multipart([ ttp.encode(
            "utf-8"), pti.encode("utf-8"), count_pub, count_sub])
