package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.io.values.PlainValuesWriter;
import com.bytedance.dancemat.io.values.ValuesWriter;

import java.io.IOException;
import java.io.OutputStream;

public class ColumnWriterImpl implements ColumnWriter {
  private ValuesWriter dataColumn;
  private ColumnDescriptor columnDescriptor;

  public ColumnWriterImpl(OutputStream os, ColumnDescriptor col) {
    dataColumn = new PlainValuesWriter(os);
    this.columnDescriptor = col;
  }

  @Override
  public void write(int value) {
    dataColumn.writeInteger(value);
  }

  @Override
  public void write(long value) {
    dataColumn.writeLong(value);
  }

  @Override
  public void write(boolean value) {
    dataColumn.writeBoolean(value);
  }

  @Override
  public void write(String value) {
    dataColumn.writeString(value);
  }

  @Override
  public void write(double value) {
    dataColumn.writeDouble(value);
  }

  @Override
  public void close() throws IOException {
    dataColumn.close();
  }

  @Override
  public long getBufferedSize() {
    return dataColumn.getBufferedSize();
  }

  @Override
  public ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }
}
