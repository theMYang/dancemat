package com.bytedance.dancemat;

import com.bytedance.dancemat.api.ReadSupport;
import com.bytedance.dancemat.data.Record;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.SchemaParser;
import org.apache.hadoop.conf.Configuration;

public class PrimitiveReadSupport extends ReadSupport<Record> {
  @Override
  public ReadContext init(Configuration conf, MessageType fileSchema) {
    String partialSchema = conf.get(ReadSupport.DANCEMAT_READ_SCHEMA);
    MessageType requestedSchema = SchemaParser.parseSchema(partialSchema);
    return new ReadContext(requestedSchema);
  }
}
