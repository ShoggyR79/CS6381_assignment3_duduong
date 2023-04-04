h1 rm -rf /tmp/zookeeper/*
h1 /bin/bash ./zookeeper/bin/zkServer.sh start-foreground > zookeeper.out 2>&1 &
h2 python3 DiscoveryAppln.py -n disc1 -a 10.0.0.2 -z 10.0.0.1 > disc1.out 2>&1 &
h3 python3 DiscoveryAppln.py -n disc2 -a 10.0.0.3  -z 10.0.0.1 > disc2.out 2>&1 &
h4 python3 DiscoveryAppln.py -n disc3 -a 10.0.0.4  -z 10.0.0.1 > disc3.out 2>&1 &
h5 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.5" -T 5 -n pub5 -z 10.0.0.1 > pub5.out 2>&1 &  
h6 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.6" -T 5 -n pub6 -z 10.0.0.1> pub6.out 2>&1 &  
h7 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.7" -T 5 -n pub7 -z 10.0.0.1> pub7.out 2>&1 & 
h8 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub1 -f "output.csv" -z 10.0.0.1> sub1.out 2>&1 &
h9 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 -f "output.csv" -z 10.0.0.1> sub2.out 2>&1 &
h10 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub3 -f "output.csv" -z 10.0.0.1> sub3.out 2>&1 &
h11 python3 BrokerAppln.py -a "10.0.0.11" -n broker1 -z 10.0.0.1 > broker1.out 2>&1 &