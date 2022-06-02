# pub-sub-system

Consists of a broker manger, brokers and publishers and subscribers.
Implements algorithms to demonstrate a pub-sub architecture with fault tolerance.

Compile Instructions 
1. Compile each java Server class individually
2. Atleast one broker should be running before the broker manager is instantialed
3. Run brokers on port 6000, 6001 and 6002(Only three have been accounted for as of now - more can be added later) using java Broker <Port number>
4. Instantiate broker manager using java BrokerManager
5. Run publisher using java Publisher
6. Run subscribers using java Subscriber <Port number>.
7. Publish topics using pub <topic> <information>
8. Subscribe to topics using addsub <topic>
  
