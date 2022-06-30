package com.bytedance.dancemat.io;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.api.WriteSupport;
import com.bytedance.dancemat.data.Record;
import com.bytedance.dancemat.data.RecordWriter;
import com.bytedance.dancemat.schema.MessageType;

public class PrimitiveWriteSupport extends WriteSupport<Record> {
  private final MessageType schema;
  private RecordWriter recordWriter;

  public PrimitiveWriteSupport(MessageType schema) {
    this.schema = schema;
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordWriter = new RecordWriter(recordConsumer, schema);
  }

  @Override
  public void write(Record record) {
    this.recordWriter.write(record);
  }

  @Override
  public MessageType getSchema(){
    return schema;
  }
}
