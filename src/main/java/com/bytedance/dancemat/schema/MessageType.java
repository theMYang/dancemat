package com.bytedance.dancemat.schema;

import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString
public class MessageType implements Type {
  private final String name;
  private List<Type> fields;
  private Map<String, Integer> indexByName;

  public MessageType(String name, List<Type> fields) {
    this.name = name;
    this.fields = fields;
    this.indexByName = new HashMap<String, Integer>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  public MessageType(String name, List<Type> fields, Map<String, Integer> indexByName) {
    this.name = name;
    this.fields = fields;
    this.indexByName = indexByName;
  }

  @Override
  public String getName() {
    return name;
  }

  public int getFieldIndex(String name) {
    if (!indexByName.containsKey(name)) {
      throw new RuntimeException(name + " not found in " + this);
    }
    return indexByName.get(name);
  }

  public List<Type> getFields() {
    return fields;
  }

  public Type getType(int idx) {
    if (idx >= fields.size()) {
      throw new IllegalArgumentException(idx + " exceed schema length " + fields.size());
    }
    return fields.get(idx);
  }

  public int getFieldCount() {
    return fields.size();
  }

  public void addField(PrimitiveType type) {
    fields.add(type);
    indexByName.put(type.getName(), getFieldCount() - 1);
  }
}
