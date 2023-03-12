from kazoo.client import KazooClient
from topic_selector import TopicSelector

zk = KazooClient(hosts='localhost:2181')
zk.start()

# Create the root znode structure
zk.ensure_path('/topics')
zk.ensure_path('/leader')
zk.ensure_path('/broker')

# Create child znodes for each topic
ts = TopicSelector()
topiclist = ts.all()
for topic in topiclist:
    zk.ensure_path(f'/topics/{topic}')
    zk.ensure_path(f'/topics/{topic}/pub')
    zk.ensure_path(f'/topics/{topic}/sub')

    @zk.ChildrenWatch(f'/topics/{topic}/sub')
    def watch_subscribers(children):
        print(f'Topic {topic} subscribers updated: {children}')

# Create child znodes for each routing broker
brokers = ['rb1', 'rb2', 'rb3']
for broker in brokers:
    zk.ensure_path(f'/broker/{broker}')

# Set watch functions for topics and subscribers
@zk.ChildrenWatch('/topics')
def watch_topics(children):
    for topic in children:
        # Watch the subscribers list for changes
        zk.get(f'/topics/{topic}/sub', watch=watch_subscribers)


@zk.ChildrenWatch('/topics/{topic}/sub')
def watch_subscribers(children):
    print(f'Topic {topic} subscribers updated: {children}')