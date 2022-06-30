package com.bytedance.dancemat.io;

import com.bytedance.dancemat.io.column.ColumnDescriptor;
import com.bytedance.dancemat.io.column.ColumnIO;
import com.bytedance.dancemat.schema.PrimitiveType;

public class PrimitiveColumnIO extends ColumnIO {
  private final ColumnDescriptor columnDescriptor;
  public PrimitiveColumnIO(PrimitiveType schema) {
    super(schema);
    columnDescriptor = new ColumnDescriptor(schema);
  }

  public ColumnDescriptor getColumnDescriptor() {
    return columnDescriptor;
  }
}
