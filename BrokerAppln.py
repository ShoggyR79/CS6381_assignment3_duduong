###############################################
#
# Author: Aniruddha Gokhale
# Vanderbilt University
#
# Purpose: Skeleton/Starter code for the Broker application
#
# Created: Spring 2023
#
###############################################


# This is left as an exercise for the student.  The Broker is involved only when
# the dissemination strategy is via the broker.
#
# A broker is an intermediary; thus it plays both the publisher and subscriber roles
# but in the form of a proxy. For instance, it serves as the single subscriber to
# all publishers. On the other hand, it serves as the single publisher to all the subscribers.
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
from CS6381_MW.BrokerMW import BrokerMW
# We also need the message formats to handle incoming responses.
from CS6381_MW import discovery_pb2

# import any other packages you need.
from enum import Enum  # for an enumeration we are using to describe what state we are in


class BrokerAppln():
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        LOOKUP = 4,
        DISSEMINATE = 5,
        COMPLETED = 6
    
    def __init__(self, logger):
        self.state = self.State.INITIALIZE
        self.name = None
        self.topiclist = None
        self.lookup = None
        self.dissemination = None
        self.mw_obj = None
        self.logger = logger
        
    def configure(self, args):
        try:
            self.logger.info("BrokerAppln::configure")
            
            # set our state
            self.state = self.State.CONFIGURE
            
            #init variables
            self.name = args.name
            
            #get config object
            self.logger.debug("BrokerAppln::configure - parsing config.ini")
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            
            self.logger.debug("BrokerAppln::configure - creating topic list")
            ts = TopicSelector()
            self.topiclist = ts.all()
            
            # setup the middleware object
            self.logger.debug("BrokerAppln::configure - creating middleware object")
            self.mw_obj = BrokerMW(self.logger)
            self.logger.debug("BrokerAppln::driver - upcall handler")
            self.mw_obj.set_upcall_handle(self)
            self.mw_obj.configure(args) # pass remainder of args to the m/w object
            
            self.logger.info("BrokerAppln::configure - completed")
        except Exception as e:
            raise e
    
    def driver (self):
        try:
            self.logger.info("BrokerAppln::driver")
            self.dump()
            
            
            self.mw_obj.setWatch()
            self.state = self.State.REGISTER
            
            self.mw_obj.event_loop(timeout = 0)
            self.logger.info("BrokerAppln::driver - completed")
        except Exception as e:
            raise e
   
    def invoke_operation(self):
        try:
            self.logger.info("BrokerAppln::invoke_operation")
            self.logger.info("BrokerAppln::invoke_operation - state: {}".format(self.state))
            if self.state == self.State.REGISTER:
                # send a register msg to discovery service
                self.logger.debug ("PublisherAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register (self.name, self.topiclist)
                self.state = self.State.LOOKUP
                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a register request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
                return 0
            elif (self.state == self.State.ISREADY):
                # Now keep checking with the discovery service if we are ready to go
                #
                # Note that in the previous version of the code, we had a loop. But now instead
                # of an explicit loop we are going to go back and forth between the event loop
                # and the upcall until we receive the go ahead from the discovery service.
                
                self.logger.debug ("PublisherAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready ()  # send the is_ready? request

                # Remember that we were invoked by the event loop as part of the upcall.
                # So we are going to return back to it for its next iteration. Because
                # we have just now sent a isready request, the very next thing we expect is
                # to receive a response from remote entity. So we need to set the timeout
                # for the next iteration of the event loop to a large num and so return a None.
            elif self.state == self.State.LOOKUP:
                self.logger.debug("BrokerAppln::invoke_operation - lookup")
                self.mw_obj.lookup(self.topiclist)
                return None
        except Exception as e:
            raise e
        
    
    def dump(self):
        try:
            self.logger.info ("**********************************")
            self.logger.info ("BrokerAppln::dump")
            self.logger.info ("------------------------------")
            self.logger.info ("     Name: {}".format (self.name))
            self.logger.info ("     Lookup: {}".format (self.lookup))
            self.logger.info ("     Dissemination: {}".format (self.dissemination))
            self.logger.info ("     TopicList: {}".format (self.topiclist))
            self.logger.info ("**********************************")

        except Exception as e:
            raise e
###################################
#
# Parse command line arguments
#
###################################
def parseCmdLineArgs ():
  # instantiate a ArgumentParser object
  parser = argparse.ArgumentParser (description="Publisher Application")
  
  # Now specify all the optional arguments we support
  # At a minimum, you will need a way to specify the IP and port of the lookup
  # service, the role we are playing, what dissemination approach are we
  # using, what is our endpoint (i.e., port where we are going to bind at the
  # ZMQ level)
  
  parser.add_argument ("-n", "--name", default="broker", help="Some name assigned to broker")

  parser.add_argument ("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")

  parser.add_argument ("-p", "--port", type=int, default=7777, help="Port number on which our underlying publisher ZMQ service runs, default=7777")
    
  parser.add_argument ("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default localhost:5555")

  parser.add_argument ("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")

  parser.add_argument ("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
  
  parser.add_argument ("-z", "--zookeeper", default="localhost:2181", help="IP Addr:Port combo for the zookeeper service, default is localhost:2181")

  return parser.parse_args()


def main():
    try:
        logging.info("Main - acqurie a child logger and then log messages in child")
    # obtain a system wide logger and initialize it to debug level to begin with
        logging.info ("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger ("PublisherAppln")
        
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs ()

        # reset the log level to as specified
        logger.debug ("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel (args.loglevel)
        logger.debug ("Main: effective log level is {}".format (logger.getEffectiveLevel ()))

        # Obtain a publisher application
        logger.debug ("Main: obtain the publisher appln object")
        broker_app = BrokerAppln (logger)

        # configure the object
        logger.debug ("Main: configure the publisher appln object")
        broker_app.configure (args)

        # now invoke the driver program
        logger.debug ("Main: invoke the publisher appln driver")
        broker_app.driver ()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

###############################################
#
# Main entry point
#
###############################################
if __name__ == "__main__":
    # set underlying defautl logging capability
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    main()
