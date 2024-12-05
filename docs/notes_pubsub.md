


# Broker
Proxy for creating topics


# Topics 
- Stores Messages 

Publish()
Subscribe("consumer1", handler) 

Model: 
- TopicMessage
- Topic

TopicRepository
   - SaveMessage("topic1", message)
   - GetMessagesFromOffsetWithLimit("topic1")
   - GetMessagesOlderThan("topic1", time)

# Subscription
Model: 
- Subscription
- SubscriptionMessage


