package com.bytedance.dancemat.api;

import com.bytedance.dancemat.io.file.InputFile;
import com.bytedance.dancemat.schema.MessageType;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

public class ReadSupport<T> {
  public static final String DANCEMAT_READ_SCHEMA = "dancemat.read.schema";
  public ReadContext init(Configuration conf, MessageType fileSchema) {
    throw new UnsupportedOperationException("Override init(InitContext)");
  }

  public static final class ReadContext {
    private final MessageType requestedSchema;

    public ReadContext(MessageType requestedSchema) {
      if (requestedSchema == null) {
        throw new NullPointerException("requestedSchema");
      }
      this.requestedSchema = requestedSchema;
    }

    public MessageType getRequestedSchema() {
      return requestedSchema;
    }
  }
}
