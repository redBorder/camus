package com.liquidm.camus;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.liquidm.Events;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import org.codehaus.jackson.map.ObjectMapper;

public class KafkaJSONProtobufDecoder extends MessageDecoder<byte[], Events.EventLogged> {
  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public CamusWrapper<Events.EventLogged> decode(byte[] message) {
    try {
      Events.EventLogged.Builder event = Events.EventLogged.newBuilder();
      JsonIOUtil.mergeFrom(message, event, RuntimeSchema.getSchema(Events.EventLogged.Builder.class), false);

      return new CamusWrapper<Events.EventLogged>(event.build(), ((long) event.getTimestamp()) * 1000);
    } catch (Exception e) {
      throw new MessageDecoderException("Unable to deserialize event: " + message.toString(), e);
    }
  }
}