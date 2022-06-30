package com.bytedance.dancemat.data;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.schema.MessageType;
import lombok.ToString;

@ToString
public class SimpleRecord extends Record {
  private final MessageType schema;
  private final Primitive[] data;

  public SimpleRecord(MessageType schema) {
    this.schema = schema;
    this.data = new Primitive[schema.getFieldCount()];
  }

  private Object getValue(int field) {
    return data[field];
  }

  private void add(int field, Primitive value) {
    data[field] = value;
  }

  @Override
  public void add(int fieldIndex, int value) {
    add(fieldIndex, new IntegerValue(value));
  }

  @Override
  public void add(int fieldIndex, long value) {
    add(fieldIndex, new LongValue(value));
  }

  @Override
  public void add(int fieldIndex, String value) {
    add(fieldIndex, new StringValue(value));
  }

  @Override
  public void add(int fieldIndex, boolean value) {
    add(fieldIndex, new BooleanValue(value));
  }

  @Override
  public void add(int fieldIndex, double value) {
    add(fieldIndex, new DoubleValue(value));
  }

  @Override
  public MessageType getType() {
    return schema;
  }

  @Override
  public void writeValue(int field, RecordConsumer recordConsumer) {
    ((Primitive) getValue(field)).writeValue(recordConsumer);
  }
}
