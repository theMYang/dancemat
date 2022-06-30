package com.bytedance.dancemat.io;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.api.WriteSupport;
import com.bytedance.dancemat.io.column.ColumnDescriptor;
import com.bytedance.dancemat.io.column.ColumnWriter;
import com.bytedance.dancemat.io.file.DancematFileWriter;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.utils.Constants;
import com.bytedance.dancemat.utils.Properties;

import java.io.IOException;
import java.util.Objects;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class InternalRecordWriter<T> {

  private final DancematFileWriter fileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final long blockSize;
  private final Properties props;
  private RecordConsumer recordConsumer;
  private RecordColumnIO columnIO;
  private long recordCount = 0;

  private boolean closed;

  private long recordCountForNextMemCheck = Constants.MINIMUM_RECORD_COUNT_FOR_CHECK;

  public InternalRecordWriter(
      DancematFileWriter fileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Properties props) {
    this.fileWriter = fileWriter;
    this.writeSupport = Objects.requireNonNull(writeSupport);
    this.schema = schema;
    this.blockSize = props.getBlockSize();
    this.props = props;
    initStore();
  }

  private void initStore() {
    this.columnIO = new RecordColumnIO(schema, fileWriter.getFileOutStream());
    this.recordConsumer = columnIO.newRecordWriter();
    writeSupport.prepareForWrite(recordConsumer);
  }

  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++recordCount;
    checkBlockSizeReached();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      // 增加从recordConsumer 中拿各ColumnWriter 存储空间判断是否flush block
      long bytesWritten = columnIO.getRecordWriter().bytesWritten();
      if (bytesWritten >= blockSize) {
        flushRowGroupToStore();
        initStore();
      }
    }
  }

  private void flushRowGroupToStore()
      throws IOException {
    recordConsumer.flush();
    if (recordCount > 0) {
      fileWriter.startBlock(recordCount);

      ColumnWriter[] columnWriters = recordConsumer.getColumnWriters();
      for (ColumnWriter w : columnWriters) {
        w.close();
      }
      for (ColumnWriter w : columnWriters) {
        ColumnDescriptor columnDescriptor = w.getColumnDescriptor();
        fileWriter.startColumn(columnDescriptor, recordConsumer.getValueCount(), w.getBufferedSize(), recordConsumer.getFirstDataOffset());
        fileWriter.endColumn();
      }
      recordCount = 0;
      fileWriter.endBlock();
    }
  }

  public void close() throws IOException {
    if (!closed) {
      flushRowGroupToStore();
      fileWriter.end();
      closed = true;
    }
  }
}
