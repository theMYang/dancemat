package com.bytedance.dancemat.data;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.Type;

public class RecordWriter {
  private final RecordConsumer recordConsumer;
  private final MessageType schema;

  public RecordWriter(RecordConsumer recordConsumer, MessageType schema) {
    this.recordConsumer = recordConsumer;
    this.schema = schema;
  }

  public void write(Record record) {
    recordConsumer.startMessage();
    writeRecord(record);
    recordConsumer.endMessage();
  }

  private void writeRecord(Record record) {
    int fieldCount = schema.getFieldCount();
    for (int field = 0; field < fieldCount; ++field) {
      Type type = schema.getType(field);
      String fieldName = type.getName();
      recordConsumer.startField(fieldName, field);
      record.writeValue(field, recordConsumer);
      recordConsumer.endField(fieldName, field);
    }
  }
}
