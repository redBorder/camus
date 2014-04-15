package com.liquidm.camus;

import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage.Builder;
import com.liquidm.Events;
import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.DoubleNode;
import org.codehaus.jackson.node.IntNode;

import java.util.Iterator;


public class KafkaJSONProtobufDecoder extends MessageDecoder<byte[], Events.EventLogged> {
  private final ObjectMapper mapper = new ObjectMapper();

  private void setValue(Builder builder, Descriptors.FieldDescriptor field, Object value) {
    if (value == null) {
      return;
    }

    if (field.isRepeated()) {
      builder.addRepeatedField(field, value);
    } else {
      builder.setField(field, value);
    }
  }

  @Override
  public CamusWrapper<Events.EventLogged> decode(byte[] message) {
    String name = null;
    try {
      final JsonNode record = mapper.readValue(message, JsonNode.class);
      Events.EventLogged.Builder event = Events.EventLogged.newBuilder();

      for (Descriptors.FieldDescriptor field : Events.EventLogged.getDescriptor().getFields()) {
        name = field.getName();
        final JsonNode value = record.get(name);

        if (value == null) {
          continue;
        }

        switch (field.getType()) {
          case STRING:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, arrayValues.next().toString());
              }
            } else {
              setValue(event, field, value.toString());
            }
            break;
          case INT64:
          case UINT64:
          case SINT64:
          case FIXED64:
          case SFIXED64:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, arrayValues.next().getValueAsLong());
              }
            } else {
              setValue(event, field, value.getValueAsLong());
            }
            break;
          case ENUM:
            Descriptors.EnumDescriptor enumType = field.getEnumType();
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, enumType.findValueByNumber(arrayValues.next().getValueAsInt()));
              }
            } else {
              setValue(event, field, enumType.findValueByNumber(value.getValueAsInt()));
            }
            break;
          case INT32:
          case UINT32:
          case SINT32:
          case FIXED32:
          case SFIXED32:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, arrayValues.next().getValueAsInt());
              }
            } else {
              setValue(event, field, value.getValueAsInt());
            }
            break;
          case DOUBLE:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, arrayValues.next().getValueAsDouble());
              }
            } else {
              setValue(event, field, value.getValueAsDouble());
            }
            break;
          case FLOAT:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, (float) arrayValues.next().getValueAsDouble());
              }
            } else {
              setValue(event, field, (float) value.getValueAsDouble());
            }
            break;
          case BOOL:
            if (value.isArray()) {
              Iterator<JsonNode> arrayValues = value.getElements();
              while(arrayValues.hasNext()) {
                setValue(event, field, arrayValues.next().getValueAsBoolean());
              }
            } else {
              setValue(event, field, value.getValueAsBoolean());
            }
            break;
          default:
            throw new UnsupportedOperationException("No support for " + field.getType());
        }
      }

      return new CamusWrapper<Events.EventLogged>(event.build(), ((long) event.getTimestamp()) * 1000);
    } catch (Exception e) {
      throw new MessageDecoderException("Unable to deserialize event: " + new String(message) + ", element " + name, e);
    }
  }
}