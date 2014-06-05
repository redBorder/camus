CLASSPATH="$(echo ./camus-liquidm/target/*selfcontained.jar):$(hadoop classpath)"
java -cp $CLASSPATH com.linkedin.camus.etl.kafka.CamusJob -P ./camus.properties
