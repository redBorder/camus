# camus
Kafka -> HDFS pipeline


If you want use the RbPartitioner, you must add this properties:

```java
etl.partitioner.class=net.redborder.camus.RbPartitioner
camus.rb.specific.path=field1
```

<b> camus.rb.specific.path: </b>  Is the specific field's name which you want use its value on HDFS path.

   Example:
 
 ```json
 {"timestamp": 1431583200, "field1":"value1", "field2":"data2"}
```

You get this HDFS path: <b> ${topic}/value1/hourly/2015/05/14/06/*.gz </b>
