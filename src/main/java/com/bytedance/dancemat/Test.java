package com.bytedance.dancemat;

import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.SchemaParser;

public class Test {
  public static void main(String[] args) {
    MessageType messageType = SchemaParser.parseSchema(
        "message test { "
            + "string binary_field "
            + "int32 int32_field "
            + "int64 int64_field "
            + "boolean boolean_field "
            + "double double_field "
            + "} ");
    System.out.println(messageType);


  }


}
