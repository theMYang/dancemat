package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.Type;

abstract public class ColumnIO {
  protected Type schema;

  public ColumnIO(Type schema){
    this.schema = schema;
  }
}
