package com.bytedance.dancemat.schema;


import lombok.Getter;
import lombok.ToString;

import java.io.IOException;

@ToString
@Getter
public class PrimitiveType implements Type {
  private final String name;
  private final PrimitiveTypeName type;

  public PrimitiveType(String name, PrimitiveTypeName type) {
    this.name = name;
    this.type = type;
  }

  @Override
  public String getName() {
    return name;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeUTF(name);
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    in.readUTF();
  }

  /**
   *  Primitive types
   */
  public static enum PrimitiveTypeName {
    INT32("getInteger", Integer.TYPE) {
    },
    INT64("getLong", Long.TYPE) {
    },
    BOOLEAN("getBoolean", Boolean.TYPE) {
    },
    STRING("getString", String.class) {
    },
    DOUBLE("getDouble", Double.TYPE) {
    };

    public final String getMethod;
    public final Class<?> javaType;

    private PrimitiveTypeName(String getMethod, Class<?> javaType) {
      this.getMethod = getMethod;
      this.javaType = javaType;
    }
  }
}
