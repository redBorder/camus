package com.liquidm.camus;

import com.liquidm.Events;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class KafkaJSONProtobufDecoder extends MessageDecoder<byte[], Events.EventLogged> {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public CamusWrapper<Events.EventLogged> decode(byte[] message) {
    try {
      final JSONRecord record = new JSONRecord(mapper.readValue(message, JsonNode.class));
      Events.EventLogged.Builder event = Events.EventLogged.newBuilder();
      event.setTimestamp((Float) record.get("timestamp"));
      event.setType(Events.EventLogged.EventType.valueOf((Integer) record.get("type")));

      return new CamusWrapper<Events.EventLogged>(event.build(), ((long) event.getTimestamp()) * 1000);
    } catch (Exception e) {
      throw new MessageDecoderException("Unable to deserialize event: " + message.toString(), e);
    }
  }
}