package com.bytedance.dancemat.io;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.io.column.ColumnIO;
import com.bytedance.dancemat.io.column.ColumnWriter;
import com.bytedance.dancemat.io.column.ColumnWriterImpl;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.PrimitiveType;
import com.bytedance.dancemat.schema.Type;
import lombok.extern.slf4j.Slf4j;

import java.io.DataOutputStream;

@Slf4j
public class RecordColumnIO extends ColumnIO {
  private final DataOutputStream outStream;
  private RecordConsumer recordWriter;

  RecordColumnIO(MessageType schema, DataOutputStream os) {
    super(schema);
    this.outStream = os;
  }

  public RecordConsumer getRecordWriter() {
    if (recordWriter == null) {
      newRecordWriter();
    }
    return this.recordWriter;
  }

  public RecordConsumer newRecordWriter() {
    this.recordWriter = new RecordColumnIOConsumer((MessageType) schema);
    return recordWriter;
  }

  private class RecordColumnIOConsumer extends RecordConsumer {
    private ColumnWriter[] columnWriters;
    private ColumnWriter currentColumnWriter;
    private long valueCount;
    private long firstDataOffset;

    public RecordColumnIOConsumer(MessageType schema) {
      columnWriters = new ColumnWriter[schema.getFieldCount()];
      this.firstDataOffset = outStream.size();
      for (int i = 0; i < schema.getFields().size(); i++) {
        Type type = schema.getFields().get(i);
        PrimitiveColumnIO primitiveColumnIO = new PrimitiveColumnIO((PrimitiveType)type);
        columnWriters[i] = new ColumnWriterImpl(outStream, primitiveColumnIO.getColumnDescriptor());
      }
    }

    @Override
    public void startMessage() {
    }

    @Override
    public void endMessage() {
      valueCount++;
    }

    @Override
    public void startField(String field, int index) {
      currentColumnWriter = columnWriters[index];
    }

    @Override
    public void endField(String field, int index) {

    }

    @Override
    public void addInteger(int value) {
      currentColumnWriter.write(value);
    }

    @Override
    public void addLong(long value) {
      currentColumnWriter.write(value);
    }

    @Override
    public void addBoolean(boolean value) {
      currentColumnWriter.write(value);
    }

    @Override
    public void addString(String value) {
      currentColumnWriter.write(value);
    }

    @Override
    public void addDouble(double value) {
      currentColumnWriter.write(value);
    }

    @Override
    public long bytesWritten() {
      long res = 0;
      for (ColumnWriter columnWriter : columnWriters) {
        res += columnWriter.getBufferedSize();
      }
      return res;
    }

    @Override
    public ColumnWriter[] getColumnWriters() {
      return columnWriters;
    }

    @Override
    public long getFirstDataOffset() {
      return firstDataOffset;
    }

    @Override
    public long getValueCount() {
      return valueCount;
    }
  }
}
