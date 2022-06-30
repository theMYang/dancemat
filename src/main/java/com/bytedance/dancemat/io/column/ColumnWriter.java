package com.bytedance.dancemat.io.column;

import java.io.IOException;

public interface ColumnWriter {
  void write(int value);

  void write(long value);

  void write(boolean value);

  void write(String value);

  void write(double value);

  default long getBufferedSize(){
    return 0L;
  }

  ColumnDescriptor getColumnDescriptor();

  void close() throws IOException;
}
