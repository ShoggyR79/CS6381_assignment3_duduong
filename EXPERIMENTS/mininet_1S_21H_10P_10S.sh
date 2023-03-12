h1 python3 DiscoveryAppln.py -cp 10 -cs 10  > discovery.out 2>&1 &
h2 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.2" -T 5 -n pub2 > pub2.out 2>&1 &  > discovery.out 2>&1 &
h3 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.3" -T 5 -n pub3 > pub3.out 2>&1 &  > discovery.out 2>&1 &
h4 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.4" -T 5 -n pub4 > pub4.out 2>&1 &  > discovery.out 2>&1 &
h5 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.5" -T 5 -n pub5 > pub5.out 2>&1 &  > discovery.out 2>&1 &
h6 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.6" -T 5 -n pub6 > pub6.out 2>&1 &  > discovery.out 2>&1 &
h7 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.7" -T 5 -n pub7 > pub7.out 2>&1 &  > discovery.out 2>&1 &
h8 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.8" -T 5 -n pub8 > pub8.out 2>&1 &  > discovery.out 2>&1 &
h9 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.9" -T 5 -n pub9 > pub9.out 2>&1 &  > discovery.out 2>&1 &
h10 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.10" -T 5 -n pub10 > pub10.out 2>&1 &  > discovery.out 2>&1 &
h11 python3 PublisherAppln.py -d "10.0.0.1:5555" -a "10.0.0.11" -T 5 -n pub11 > pub11.out 2>&1 &  > discovery.out 2>&1 &
h12 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub1 -f "output.csv"> sub1.out 2>&1 &
h13 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 -f "output.csv"> sub2.out 2>&1 &
h14 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub3 -f "output.csv"> sub3.out 2>&1 &
h15 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub4 -f "output.csv"> sub4.out 2>&1 &
h16 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub5 -f "output.csv"> sub5.out 2>&1 &
h17 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub6 -f "output.csv"> sub6.out 2>&1 &
h18 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub7 -f "output.csv"> sub7.out 2>&1 &
h19 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub8 -f "output.csv"> sub8.out 2>&1 &
h20 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub9 -f "output.csv"> sub9.out 2>&1 &
h21 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub10 -f "output.csv"> sub10.out 2>&1 &