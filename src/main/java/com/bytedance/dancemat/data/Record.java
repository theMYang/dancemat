package com.bytedance.dancemat.data;

import com.bytedance.dancemat.api.RecordConsumer;
import com.bytedance.dancemat.schema.MessageType;

public abstract class Record {

  public void add(String field, int value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, long value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, String value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, boolean value) {
    add(getType().getFieldIndex(field), value);
  }

  public void add(String field, double value) {
    add(getType().getFieldIndex(field), value);
  }

  abstract public void add(int fieldIndex, int value);

  abstract public void add(int fieldIndex, long value);

  abstract public void add(int fieldIndex, String value);

  abstract public void add(int fieldIndex, boolean value);

  abstract public void add(int fieldIndex, double value);

  public Record append(String fieldName, int value) {
    add(fieldName, value);
    return this;
  }

  public Record append(String fieldName, long value) {
    add(fieldName, value);
    return this;
  }

  public Record append(String fieldName, String value) {
    add(fieldName, value);
    return this;
  }

  public Record append(String fieldName, boolean value) {
    add(fieldName, value);
    return this;
  }

  public Record append(String fieldName, double value) {
    add(fieldName, value);
    return this;
  }

  abstract public MessageType getType();

  abstract public void writeValue(int field, RecordConsumer recordConsumer);
}
