package com.bytedance.dancemat.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.StringTokenizer;

public class SchemaParser {
  private static class Tokenizer {

    private StringTokenizer st;

    private int line = 0;
    private StringBuffer currentLine = new StringBuffer();

    public Tokenizer(String schemaString, String string) {
      st = new StringTokenizer(schemaString, " ,;{}()\n\t=", true);
    }

    public String nextToken() {
      while (st.hasMoreTokens()) {
        String t = st.nextToken();
        if (t.equals("\n")) {
          ++line;
          currentLine.setLength(0);
        } else {
          currentLine.append(t);
        }
        if (!isWhitespace(t)) {
          return t;
        }
      }
      throw new IllegalArgumentException("unexpected end of schema");
    }

    private boolean isWhitespace(String t) {
      return t.equals(" ") || t.equals("\t") || t.equals("\n");
    }

    public String getLocationString() {
      return "line " + line + ": " + currentLine.toString();
    }
  }

  public static MessageType parseSchema(String schemaString) {
    Tokenizer st = new Tokenizer(schemaString, " ;{}()\n\t");

    String t = st.nextToken();
    String name = st.nextToken();
    MessageType messageType = new MessageType(name, new ArrayList<Type>());
    addTypeFields(st, messageType);
    return messageType;
  }

  private static void addTypeFields(Tokenizer st, MessageType messageType) {
    String t = st.nextToken();
    while (!(t = st.nextToken()).equals("}")) {
      addPrimitiveType(t, st, messageType);
    }
  }

  private static void addPrimitiveType(String type, Tokenizer st, MessageType messageType) {
    String name = st.nextToken();
    PrimitiveType primitiveType = new PrimitiveType(name, asPrimitive(type));
    messageType.addField(primitiveType);
  }

  private static PrimitiveType.PrimitiveTypeName asPrimitive(String type) {
    try {
      return PrimitiveType.PrimitiveTypeName.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("expected one of " + Arrays.toString(PrimitiveType.PrimitiveTypeName.values()) + " got " + type, e);
    }
  }
}
