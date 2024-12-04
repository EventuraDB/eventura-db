Basic idea is to implement a simple pubsub system, with persistent subscriptions and messages. 
it will be used as embedded pubsub. so no network communication is needed.
storage is pebble a kv store.
we prefer consistency over performance. 

# Storage 
What is the format of the data stored? 

## Topics
Every topic contains a stream of messages. Each message has an incremental id (uint64). 

## Messages
key: message:<topic>:<id>
value: data (binary) 

key and value is represented in Message struct 

## Subscriptions 
We have subscriptions with a unique ID (string)
Each subscription is assigned to exactly one topic. 
Each subscriptions manages the offset of the messages it has read.

we have many subscriptons that read from the same topic, but in different speeeds - dependening on consumer.

Each message can be acknowledged, which means the subscription offset will be updated. 

But when we constantly poll, we need to be careful to not resend the same message. 
we load messages from topic as a batch of messages and try to inform the subscribers about the messages.

We poll every 200ms. every subscriber has its own go routine running and polling the messages. 
note: how many go routines can we run in parallel? when we startup server, we need to initialize the go routines. or we wait, until the subscriber resubscribes??

Subscriptions are handeled remotely usually. but the pubsub system we build is an embedded one. 

### sub storage and data model
key: sub:<sub_id>
value: {
    topic: <topic>,
    offset_acknowleged: <offset>, // offset of the last message that was acknowledged
    offset_inflight: <offset>, // offset of the message that is currently being processed
    count_inflight: <count>, // number of messages in flight
}

### loading of messages
Seeing this we can see, that we need to load only new messages, if count_inflight is 0. otherwise we overload the system.
Inflights can also be in retry state. 
and we need to strictly maintain a order of messages.
it would be nice to have a state per message on the subscription. like is it in transit, is it acknowledged, is it in retry state.

### message metadata and message queue.
Message metadata owned by the subscription. 
we add metadata on load messages. and follow up until acknowledgement.  
so basically its like a queue. 

Idea: one go routine filling up the queue if it runs empty, and another go routine processing the queue.
this happens for each subscription. worry: is it too many go routines running concurently? imagine having 1000 subscriptions.
ideal would be one is doing it for all.

key: sub_msgs:<sub_id>:<message_id>
value: {
    state: <state>, // state of the message
    retry_count: <count>, // number of retries
    retry_at: <time>, // time when the message should be retried
    errors  : <errors>, // errors that occured during processing
}

### Domain models vs Storage model 
I think its time to distinguish between domain models and storage models.
Does it introduce too much complexity?

What do we store on db for subscriptions? 
- ID 
- Topic








