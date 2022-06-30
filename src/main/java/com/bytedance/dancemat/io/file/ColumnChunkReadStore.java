package com.bytedance.dancemat.io.file;

import com.bytedance.dancemat.Record.RecordReader;
import com.bytedance.dancemat.data.SimpleRecord;
import com.bytedance.dancemat.io.column.Chunk;
import com.bytedance.dancemat.io.column.ColumnChunkSimpleBooleanReader;
import com.bytedance.dancemat.io.column.ColumnChunkSimpleDoubleReader;
import com.bytedance.dancemat.io.column.ColumnChunkSimpleIntReader;
import com.bytedance.dancemat.io.column.ColumnChunkSimpleLongReader;
import com.bytedance.dancemat.io.column.ColumnChunkSimpleStringReader;
import com.bytedance.dancemat.io.column.ColumnDescriptor;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.PrimitiveType.PrimitiveTypeName;

import java.util.HashMap;
import java.util.Map;

public class ColumnChunkReadStore<T> extends RecordReader {
  private final long rowCount;
  private final Map<ColumnDescriptor, ColumnChunkReader> readers = new HashMap<>();
  private final MessageType schema;
  private long currentRecord;
  SimpleRecord record;

  public ColumnChunkReadStore(long rowCount, MessageType schema) {
    this.rowCount = rowCount;
    this.schema = schema;
    this.record = new SimpleRecord(schema);
  }

  public void addColumn(ColumnDescriptor col, Chunk chunk) {
    PrimitiveTypeName type = col.getPrimitiveType().getType();
    if (PrimitiveTypeName.BOOLEAN.equals(type)) {
      readers.put(col, new ColumnChunkSimpleBooleanReader(chunk));
    } else if (PrimitiveTypeName.INT32.equals(type)) {
      readers.put(col, new ColumnChunkSimpleIntReader(chunk));
    } else if (PrimitiveTypeName.INT64.equals(type)) {
      readers.put(col, new ColumnChunkSimpleLongReader(chunk));
    } else if (PrimitiveTypeName.DOUBLE.equals(type)) {
      readers.put(col, new ColumnChunkSimpleDoubleReader(chunk));
    } else if (PrimitiveTypeName.STRING.equals(type)) {
      readers.put(col, new ColumnChunkSimpleStringReader(chunk));
    } else {
      throw new RuntimeException("Wrong type to create reader " + type);
    }
  }

  @Override
  public long getRowCount() {
    return rowCount;
  }

  @Override
  public SimpleRecord read() {
    SimpleRecord record = new SimpleRecord(schema);
    for (Map.Entry<ColumnDescriptor, ColumnChunkReader> e : readers.entrySet()) {
      ColumnChunkReader chunkReader = e.getValue();
      ColumnDescriptor col = e.getKey();
      PrimitiveTypeName type = col.getPrimitiveType().getType();
      String field = col.getPath();

      Object res = chunkReader.read();
      if (PrimitiveTypeName.BOOLEAN.equals(type)) {
        record.add(field, (Boolean) res);
      } else if (PrimitiveTypeName.INT32.equals(type)) {
        record.add(field, (Integer) res);
      } else if (PrimitiveTypeName.INT64.equals(type)) {
        record.add(field, (Long) res);
      } else if (PrimitiveTypeName.DOUBLE.equals(type)) {
        record.add(field, (Double) res);
      } else if (PrimitiveTypeName.STRING.equals(type)) {
        record.add(field, (String) res);
      }
    }
    return record;
  }


}
