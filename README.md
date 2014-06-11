# Reliable druid realtime

This branch is trying to stick druid into camus to use camus' cursor tracking to get a reliable realtime engine.

## Setup

You need to checkout git@github.com:liquidm/druid.git and do "mvn clean install" first, or the dependency on druid can't be satisfied.

## WIP

This is totally work in progress:

Here is the [core class](https://github.com/liquidm/camus/blob/camus-druid/camus-liquidm/src/main/java/com/liquidm/camus/DruidWriter.java) to be completed.

Take the [RealtimeIndexTask](https://github.com/liquidm/druid/blob/master/indexing-service/src/main/java/io/druid/indexing/common/task/RealtimeIndexTask.java) as source of inspiration of how to assemble things.

You will _not_ need a firehose and [this loop](https://github.com/liquidm/druid/blob/master/indexing-service/src/main/java/io/druid/indexing/common/task/RealtimeIndexTask.java#L329-L361) is already implicitly presented in camus, so it's ok to write to the plumber directly in [this method](https://github.com/liquidm/camus/blob/camus-druid/camus-liquidm/src/main/java/com/liquidm/camus/DruidWriter.java#L54-L55)

At the time of writing, DruidWriter is a total NOP, just opening up a Jetty server (needed for realtime queries), but this should give you an idea how lifecycle management looks like under camus control.

## Sharding

With camus, we won't know how many segments we will create in advance. Thus it seems most prudent to use linear sharding, i.e.

```JSON
  "shardSpec": {
    "type": "linear",
    "partitionNum": shard_id
  }
```

where you use a [distributed atomic long](http://curator.apache.org/curator-recipes/distributed-atomic-long.html) to have a unique shard_id. This means you need to define a zookeeper path including dataSchema name and exact hour.
