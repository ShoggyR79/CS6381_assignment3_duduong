###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Discovery application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Discovery service is a server
# and hence only responds to requests. It should be able to handle the register,
# is_ready, the different variants of the lookup methods. etc.
#
# The key steps for the discovery application are
# (1) parse command line and configure application level parameters. One
# of the parameters should be the total number of publishers and subscribers
# in the system.
# (2) obtain the discovery middleware object and configure it.
# (3) since we are a server, we always handle events in an infinite event loop.
# See publisher code to see how the event loop is written. Accordingly, when a
# message arrives, the middleware object parses the message and determines
# what method was invoked and then hands it to the application logic to handle it
# (4) Some data structure or in-memory database etc will need to be used to save
# the registrations.
# (5) When all the publishers and subscribers in the system have registered with us,
# then we are in a ready state and will respond with a true to is_ready method. Until then
# it will be false.

# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.


# Now import our CS6381 Middleware
from CS6381_MW.DiscoveryMW import DiscoveryMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in


class DiscoveryAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        WAITING = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.count_publishers = 0
        self.count_subscribers = 0
        self.mw_obj = None
        self.logger = logger
        self.lookup = None
        self.dissemination = None
        self.topics_to_publishers = None
        self.publisher_to_ip = None
        self.broker = None

    def configure(self, args):
        try:
            self.logger.info("DiscoveryAppln::configure")
            self.state = self.State.CONFIGURE
            
            # initialize our variables

            self.topics_to_publishers = {}
            self.publisher_to_ip = {}

            # get the configuration object
            self.logger.debug("DiscoveryAppln::configure - parsing {}".format(args.config))
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]

            # setup underlying middleware object
            self.logger.debug(
                "DiscoveryAppln::configure - setup underlying middleware object")
            self.mw_obj = DiscoveryMW(self.logger)
            # setting upcall handle
            self.logger.debug("DiscoveryAppln::driver - setting upcall handle")
            self.mw_obj.set_upcall_handle(self)

            # pass remainder of the args to the m/w object
            self.mw_obj.configure(args)
            self.logger.info("DiscoveryAppln::configure completed")

        except Exception as e:
            raise e

    def driver(self):
        ''' Driver program '''
        try:
            self.logger.info("DiscoveryAppln::driver")
            # dump contents
            self.dump()

            self.state = self.State.ISREADY
            self.mw_obj.create_broker_groups()
            self.mw_obj.setWatch()
            self.mw_obj.event_loop(timeout=0)

            self.logger.info("DiscoveryAppln::driver completed")

        except Exception as e:
            raise e

    def invoke_operation(self):
        ''' invoke operation depending on state '''
        try:
            self.logger.info("DiscoveryAppln::invoke_operation")
            if (self.state == self.State.WAITING or self.state == self.State.ISREADY):
                return None
            else:
                raise ValueError("undefined")
        except Exception as e:
            raise e

    # program to handle incoming register request.
    def handle_register(self, reg_req):
        ''' handle register request'''
        try:
            
            self.logger.info("DiscoveryAppln::handle_register")
            if (reg_req.role == discovery_pb2.ROLE_PUBLISHER):
                req_info = reg_req.info
                self.logger.info("DiscoveryAppln::handle_register checking if name is unique")

                if (req_info.id in self.publisher_to_ip):
                    raise Exception("Name should be unique")
                self.logger.info("DiscoveryAppln::handle_register assigning user id to info")
                self.publisher_to_ip[req_info.id] = {"id": req_info.id, "addr": req_info.addr, "port": req_info.port}
                self.logger.info("DiscoveryAppln::handle_register adding topics to publishers")
                for topic in reg_req.topiclist:
                    if topic not in self.topics_to_publishers:
                        self.topics_to_publishers[topic] = []
                    self.topics_to_publishers[topic].append(req_info.id)
                self.logger.info("DiscoveryAppln::handle_register increment number of seen publishers")

                self.count_publishers += 1
            elif (reg_req.role == discovery_pb2.ROLE_SUBSCRIBER):
                self.logger.info("DiscoveryAppln::handle_register increment number of seen subscribers")
                self.count_subscribers += 1
            elif (reg_req.role == discovery_pb2.ROLE_BOTH and self.dissemination == "Broker"):
                self.logger.info("DiscoveryAppln::handle_register broker registered and saved")
                self.broker = reg_req.info
            
            self.backup()
            self.mw_obj.register_reply( discovery_pb2.STATUS_SUCCESS)
            return None

        except Exception as e:
            raise e

  
    def isready_request(self):
        self.logger.debug("DiscoveryAppln::isready_request")
        self.logger.info ("     Count Subscribers: {}".format (self.count_subscribers))
        self.logger.info ("     Count Publishers: {}".format (self.count_publishers))
        status = (self.state == self.State.ISREADY and (self.dissemination!="Broker" or self.broker != None))
        self.mw_obj.is_ready_reply(status)
        return None
        
    def lookup_pub_by_topic_request(self, topiclist, from_broker):
        try:

            publist = []
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_request - {}".format)
            if (self.dissemination == "Broker" and not from_broker):
                publist.append(self.broker)
                return self.mw_obj.lookup_pub_by_topic_reply(publist)
            for topic in topiclist:
                self.logger.debug("DiscoveryAppln::lookup_pub_by_topic_request - topic: {}".format(topic))
                publishers = []
                if topic in self.topics_to_publishers:
                    publishers = self.topics_to_publishers[topic]
                for publisher in publishers:
                    self.logger.debug("DiscoveryAppln::lookup_pub_by_topic_request - publisher: {}".format(publisher))
                    pub_info = self.publisher_to_ip[publisher]
                    
                    publist.append(pub_info)
            self.logger.info("DiscoveryAppln::lookup_pub_by_topic_request - returning publist {}".format(publist))
            self.mw_obj.lookup_pub_by_topic_reply(publist)
            return None
        except Exception as e:
            raise e
    def backup(self):
        ''' Backup state '''
        try:
            self.logger.info("DiscoveryAppln::backup")
            self.mw_obj.send_state_to_replica(self.topics_to_publishers, self.publisher_to_ip, self.count_publishers, self.count_subscribers, self.state)
        except Exception as e:
            raise e
        
    def update_broker_info(self, broker):
        self.logger.info("DiscoveryAppln::update_broker_info - updating broker to {}".format(broker))   
        self.broker = broker
        
    def update_publisher_info(self, publishers):
        self.logger.info("DiscoveryAppln::update_publisher_info - updating publishers")
        self.publisher_to_ip = {}
        self.topics_to_publishers = {}
        for pub in publishers:
            info = pub["id"]
            topiclist = pub["topiclist"]
            self.publisher_to_ip[info["id"]] = info
            for topic in topiclist:
                if topic not in self.topics_to_publishers:
                    self.topics_to_publishers[topic] = []
                self.topics_to_publishers[topic].append(info["id"])

        self.logger.info("DiscoveryAppln::update_publisher_info - updated publishers: {}".format(self.publisher_to_ip))
        self.logger.info("DiscoveryAppln::update_publisher_info - updated topics: {}".format(self.topics_to_publishers))
            
            
            
    def update_state(self, topics_to_publisher, publisher_to_ip, count_pub, count_sub):
        self.topics_to_publishers = topics_to_publisher
        self.publisher_to_ip = publisher_to_ip
        self.count_publishers = count_pub
        self.count_subscribers = count_sub
        
    def dump(self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("DiscoveryAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e

def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Discovery Application")


    parser.add_argument("-a", "--addr", default="localhost",
                        help="IP addr of this discovery to advertise (default: localhost)")
    parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per discovery")


    parser.add_argument("-p", "--port", type=int, default=5555,
                        help="Port number for underlying discovery ZMQ, default=5557")
    parser.add_argument("-q", "--quorum", type=int, default=3,
                        help="Expected number of discovery nodes in the quorum, default=3")

    parser.add_argument("-c", "--config", default="config.ini",
                        help="Configuration file, default=config.ini")
    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IP Addr:Port combo for the zookeeper service, default is localhost:2181")

    
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[
                        logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    
    parser.add_argument("-g", "--groups", type=int, default=3,
                        help="Number of groups to divide the brokers into, default=3")

    return parser.parse_args()

# main program


def main():
    try:
        logging.info(
            "Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")  # get a child logger

        # first parse the arguments
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()

        # # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(
            logger.getEffectiveLevel()))

        # obatin a discovery application
        logger.debug("Main: obtain a discovery application object")
        dis_app = DiscoveryAppln(logger)

        # configure the object
        logger.debug("Main: configure the discovery application object")
        dis_app.configure(args)

        # now invoke the driver program
        logger.debug("Main: invoke the discovery application driver")
        dis_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))


# main entry point
if __name__ == "__main__":
    # set underlying default logging capability
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(name)s %(levelname)s - %(message)s')
    main()
