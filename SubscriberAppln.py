###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the subscriber application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise to the student. Design the logic in a manner similar
# to the PublisherAppln. As in the publisher application, the subscriber application
# will maintain a handle to the underlying subscriber middleware object.
#
# The key steps for the subscriber application are
# (1) parse command line and configure application level parameters
# (2) obtain the subscriber middleware object and configure it.
# (3) As in the publisher, register ourselves with the discovery service
# (4) since we are a subscriber, we need to ask the discovery service to
# let us know of each publisher that publishes the topic of interest to us. Then
# our middleware object will connect its SUB socket to all these publishers
# for the Direct strategy else connect just to the broker.
# (5) Subscriber will always be in an event loop waiting for some matching
# publication to show up. We also compute the latency for dissemination and
# store all these time series data in some database for later analytics.


# import the needed packages
import os     # for OS functions
import sys    # for syspath and system exception
import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.

# Import our topic selector. Feel free to use alternate way to
# get your topics of interest
from topic_selector import TopicSelector

# Now import our CS6381 Middleware
from CS6381_MW.SubscriberMW import SubscriberMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in


class SubscriberAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        COMPLETED = 5
        
    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.num_topics = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger 

    # configure/init 
    def configure(self, args):
        '''Initialize the object'''
        try:
            self.logger.info("SubscriberAppln::configure")
            
            # set our state
            self.state = self.State.CONFIGURE
            
            # init variables
            self.num_topics = args.num_topics 
            self.name = args.name
            
            # get the configuration object
            self.logger.debug("SubscriberAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            
            # get the topic list
            self.logger.debug("SubscriberAppln::configure - get the topic list")
            ts = TopicSelector()
            self.topiclist = ts.interest(self.num_topics)
            
            # Now setup our underlying middleware object
            self.logger.debug("SubscriberAppln::configure - setup the middleware object")
            self.mw_obj = SubscriberMW(self.logger)
            self.logger.debug("SubscriberAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.configure(args, self.dissemination, self.topiclist)
            self.logger.info("SubscriberAppln::configure - completed")
            
        except Exception as e:
            raise e
    
    def driver(self):
        ''' Driver program '''
        try:
            self.logger.info("SubscriberAppln::driver")
            self.dump()
            
            
            
            self.state = self.State.REGISTER
            self.mw_obj.setWatch()
            self.mw_obj.event_loop(timeout = 0)
            self.logger.info("SubscriberAppln::driver - completed")
        except Exception as e:
            raise e
        
    def invoke_operation(self):
        try:
            self.logger.info("SubscriberAppln::invoke_operation")
            
            # check what state are we in. 
            if (self.state == self.State.REGISTER):
                return None
            elif (self.state == self.State.LOOKUP):
                self.logger.debug("SubscriberAppln::invoke_operation - send LOOKUP msg to discovery service")
                # implement in milestone 2
                # self.mw_obj.lookup(self.topiclist)
                return None
            elif (self.state == self.State.COMPLETED):
                self.logger.debug("SubscriberAppln::invoke_operation - COMPLETED")
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError("Undefined state of the appln object")
                
        except Exception as e:
            raise e
  

    def update_publishers_info(self, publist):
        self.logger.info("SubscriberAppln::update_publishers_info")
        filtered_pubs = []
        for pub in publist:
            # if there is any overlap between the topics we are interested in and the topics the publisher is publishing
            if (set(self.topiclist).intersection(pub['topiclist'])):
                filtered_pubs.append(pub['id'])
        self.mw_obj.subscribe(filtered_pubs)
    ########################################
    # dump the contents of the object 
    ########################################
    def dump (self):
        ''' Pretty print '''

        try:
            self.logger.info ("**********************************")
            self.logger.info ("PublisherAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     Num Topics: {}".format (self.num_topics))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e
# parse command line arguments
def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Subscriber Application")
    parser.add_argument ("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")
    parser.add_argument ("-f", "--filename", default="output.csv", help="output file name (default: output.csv)")

    parser.add_argument("-c", "--config", default="config.ini",
                          help="configuration file (default: config.ini)")

    parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

    parser.add_argument ("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")

    parser.add_argument ("-l", "--loglevel", type=int, default=logging.DEBUG, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")

    parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IP Addr:Port combo for the zookeeper service, default is localhost:2181")

    parser.add_argument ("-hs", "--h_size", type=int, default=10, help="Size of the history buffer, default is 10")
    return parser.parse_args()
# main program
def main():
    try:
        logging.info("Main - acquire a child logger")
        logger = logging.getLogger("SubscriberAppln")  # get a child logger
        
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs ()

        # reset the log level to as specified
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        
        #obtain a subscriber application
        logger.debug("Main: obtain a subscriber application")
        sub_app = SubscriberAppln(logger)
        
        #configure the application
        logger.debug("Main: configure the application")
        sub_app.configure(args)
        
        #invoke the driver program
        logger.debug("Main: invoke the driver program")
        sub_app.driver()
        
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return


# main entry point
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
