# Kafka Workshop

# Setup

Start Kafka:

    docker-compose up -d
    
Bash into the docker container to access the command line tools:

    docker-compose exec workshop-kafka bash
    
Conversely you can download the kafka [binaries](https://kafka.apache.org/downloads.html) and use the
cli tools included in the folder `bin`. You'll need to add this entry into your `/etc/hosts` file:

    127.0.0.1 workshop-kafka

> **IMPORTANT** You should always use the latest client version regardless of server version 
>(unless it's prior to version 0.10.2). Besides bug fixes this allows us to upgrade client deps before upgrading the cluster.
>https://www.confluent.io/blog/upgrading-apache-kafka-clients-just-got-easier/
    
# Command Line Tools

## Simple topic

Topics can be managed with the `kafka-topics.sh` script:
    
    kafka-topics.sh --help
    
Create a simple topic with 1 partition and a replication factor of 2:

    kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --topic simple.topic \
        --create \
        --partitions 1 \
        --replication-factor 2
    
_WOOP!_ We are not allowed to create a replication factor greater than the number of brokers in our cluster.

> **INFO** In Kafka nodes are called _brokers_ or a _Kafka broker_ while the cluster is called a _Kafka cluster_.

Try again with a RF of 1:

    kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --topic simple.topic \
        --create \
        --partitions 1 \
        --replication-factor 1

Should've created with the warning:

    WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.
    
You shouldn't mix `_` and `.` in your project. Standardize around one of those or avoid them completely.

> **INFO** all kafka cli commands that expect a broker endpoint can be supplied as a comma separated list.

Ok, let's produce some messages:

    kafka-console-producer.sh  \
        --broker-list localhost:9092 \
        --topic simple.topic 
    
This will open up a console that you can input messages. Messages will be sent after each new-line. Type something fun 
but make sure to input 3 or more messages:

    friends
    don't
    let
    friends
    judge
    a
    language
    by
    hello world
    

Open a second terminal and set up a consumer:

    kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic simple.topic

This will block the terminal waiting for messages... _but what? where are our messages?!!!_
`ctrl-c` out of the consumer and let's read from the beginning

    kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic simple.topic \
        --from-beginning

Your message should appear now. Notice that the messages are in order (one partition gives us global order)
You can type in more messages into the producer and they should appear in the consumer in real time.

You can also start a consumer from a provided offset:

    kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 \
         --topic simple.topic \
         --partition 0 \
         --offset 3
         
Let's try a consumer with subscription. This is done by identifying your consumer as part of a group:
    
        kafka-console-consumer.sh \
             --bootstrap-server localhost:9092 \
             --topic simple.topic \
             --consumer-property group.id=simple.consumer1 \
             --from-beginning

It should spit out the contents of the topic. Kill the consumer and add in more messages. After that rerun the
previous consumer command. It should start from it left off, even with the `--from-beginning`.

We can get information about the the consumer group with the following command:

       kafka-consumer-groups.sh \
            --bootstrap-server localhost:9092 \
            --group simple.consumer1 \
            --describe

You can also delete the consume group:

        kafka-consumer-groups.sh \
                    --bootstrap-server localhost:9092 \
                    --group simple.consumer1 \
                    --delete 
    
## Multi partition Topics
    
`ctrl-c` out of the consumers and producers and let's test out a topic with 3 partitions:

    kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --topic part3.topic \
        --create \
        --partitions 3 \
        --replication-factor 1
        
> **INFO**: Kafka may allow you to auto create topics when setting up a producer/consumer with
> broker configured defaults. Most production kafka setups will not allow you to do this.

Start a producer and create 12 messages, 1-12:

    kafka-console-producer.sh  \
        --broker-list localhost:9092 \
        --topic part3.topic 
    
Start up a consumer reading from the beginning:

    kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic part3.topic \
        --from-beginning

_What happened to the order!!!?_ Messages without keys (the ones we are generating) get sent to the
producers in a Round-Robin fashion. Order is only preserved within a partition. The consumer will batch
receive the messages from one partition before moving onto the next.    
    
Let's try some keys. `ctrl-c` out from the producer and consumer and delete the topic:

    kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --topic part3.topic \
        --delete
        
Recreate the topic and setup a new producer:

    kafka-console-producer.sh  \
        --broker-list localhost:9092 \
        --topic part3.topic \
        --property "parse.key=true" \
        --property "key.separator=:"
        
Write in your messages like this (don't copy paste all of them!):

       0:1
       a:a
       X:x
       0:2
       a:b
       X:y
       0:3
       a:c
       X:z
 
 Then try the consumer again:
 
     kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 \
         --topic part3.topic \
         --from-beginning
         
You should now notice that the order is persevered within the partitioned groups (1-3, a-c, x-z).
Let's read from the partitions individually:

     kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 \
         --topic part3.topic \
         --from-beginning \
         --partition 0

Try it out with partitions `1` and `2`.

_It would be nice to see the key in the console._ It sure would! Try this consumer out:

     kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 \
         --topic part3.topic \
         --from-beginning \
         --property "print.key=true"

    
# Code

Time to check out some code. Start with `AConsumerSimple` and head on down. You will need to
add the following to your `/etc/hosts` file:

    127.0.0.1 workshop-kafka
    
At any moment the kafka docker can be reset with this command

    docker-compose rm -svf
    
This will kill the container so you'll need to recreate it again.

# Pit Falls

## Watch out for slow consumers

Subscribers set up in consumer groups will send a `heartbeat` on every `poll`. Remember, 
Kafka does everything in batches. If your consumers are slow it might take too long between
polls which will cause consumer re-balance. This will further slow down your processing as well
as lead to receiving duplicate messages. By default `poll` will receive no more than 500 messages
and a `heartbeat` is expected every 5 minutes. This can be configured with the following consumer
properties:

    max.poll.interval.ms
    max.poll.records
    
## Be mindful of subscribers

Do you really need a subscriber? It's not always necessary. If you need to start off from where
you last left off go for it. On the other hand if you only care about the latest messages or you
always start from the beginning handle the consumer yourself with `assign` and `seek`. [More on subscribers](https://dzone.com/articles/dont-use-apache-kafka-consumer-groups-the-wrong-wa)
    
## Be mindful of setting up a cleanup policy

By default topics will retain all messages. Make sure this is what you want. Otherwise setup
`delete` or `compact` (or both comma separated) in the topic config `cleanup.policy`.

## Configure segment size properly

Kafka saves all records into segments. By default a segment is closed once it reaches 1 week in age or
1GB in size, whichever comes first. This might trouble your cleanup configuration. If you're using the `delete`
cleanup policy a single unexpired records will keep an entire segment from being deleted. This policy acts
on entire segments and not individual records.

`compact` on the other hand will cause segments to merge if you are using `delete` and `compact` this can cause
very old messages to stick around since `compact` will run before `delete`. An old message might always find 
itself in a segment with new messages. [More on log compaction](https://dzone.com/articles/kafka-architecture-log-compaction) 

Here are the relevant topic configs:

    segment.bytes
    segment.ms
    
    
# Going Further

Checkout the _Apache Kafka Series_ by Stephane Maarek on Udemy. He has lots of courses on Kafka.