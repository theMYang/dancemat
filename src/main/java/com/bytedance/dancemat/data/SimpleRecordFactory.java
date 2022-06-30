package com.bytedance.dancemat.data;

import com.bytedance.dancemat.schema.MessageType;

public class SimpleRecordFactory extends RecordFactory {
  private final MessageType schema;

  public SimpleRecordFactory(MessageType schema) {
    this.schema = schema;
  }

  @Override
  public Record newRecord() {
    return new SimpleRecord(schema);
  }
}
