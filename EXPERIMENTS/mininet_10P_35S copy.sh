h1 python3 DiscoveryAppln.py -cp 10 -cs 35  > discovery.out 2>&1 &
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
h12 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub1 -f "output0.csv"> sub1.out 2>&1 &
h13 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub2 -f "output0.csv"> sub2.out 2>&1 &
h14 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub3 -f "output0.csv"> sub3.out 2>&1 &
h15 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub4 -f "output0.csv"> sub4.out 2>&1 &
h16 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub5 -f "output0.csv"> sub5.out 2>&1 &
h17 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub6 -f "output0.csv"> sub6.out 2>&1 &
h18 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub7 -f "output0.csv"> sub7.out 2>&1 &
h19 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub8 -f "output0.csv"> sub8.out 2>&1 &
h20 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub9 -f "output0.csv"> sub9.out 2>&1 &
h21 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub10 -f "output1.csv"> sub10.out 2>&1 &
h22 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub11 -f "output1.csv"> sub11.out 2>&1 &
h23 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub12 -f "output1.csv"> sub12.out 2>&1 &
h24 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub13 -f "output1.csv"> sub13.out 2>&1 &
h25 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub14 -f "output1.csv"> sub14.out 2>&1 &
h26 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub15 -f "output1.csv"> sub15.out 2>&1 &
h27 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub16 -f "output1.csv"> sub16.out 2>&1 &
h28 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub17 -f "output1.csv"> sub17.out 2>&1 &
h29 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub18 -f "output1.csv"> sub18.out 2>&1 &
h30 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub19 -f "output1.csv"> sub19.out 2>&1 &
h31 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub20 -f "output2.csv"> sub20.out 2>&1 &
h32 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub21 -f "output2.csv"> sub21.out 2>&1 &
h33 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub22 -f "output2.csv"> sub22.out 2>&1 &
h34 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub23 -f "output2.csv"> sub23.out 2>&1 &
h35 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub24 -f "output2.csv"> sub24.out 2>&1 &
h36 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub25 -f "output2.csv"> sub25.out 2>&1 &
h37 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub26 -f "output2.csv"> sub26.out 2>&1 &
h38 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub27 -f "output2.csv"> sub27.out 2>&1 &
h39 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub28 -f "output2.csv"> sub28.out 2>&1 &
h40 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub29 -f "output2.csv"> sub29.out 2>&1 &
h41 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub30 -f "output3.csv"> sub30.out 2>&1 &
h42 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub31 -f "output3.csv"> sub31.out 2>&1 &
h43 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub32 -f "output3.csv"> sub32.out 2>&1 &
h44 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub33 -f "output3.csv"> sub33.out 2>&1 &
h45 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub34 -f "output3.csv"> sub34.out 2>&1 &
h46 python3 SubscriberAppln.py -d "10.0.0.1:5555" -T 5 -n sub35 -f "output3.csv"> sub35.out 2>&1 &