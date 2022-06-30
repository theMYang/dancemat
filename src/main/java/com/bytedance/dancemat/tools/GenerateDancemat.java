package com.bytedance.dancemat.tools;

import com.bytedance.dancemat.DancematWriter;
import com.bytedance.dancemat.data.Record;
import com.bytedance.dancemat.data.SimpleRecordFactory;
import com.bytedance.dancemat.io.PrimitiveWriteSupport;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.SchemaParser;
import com.bytedance.dancemat.utils.HadoopStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class GenerateDancemat {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    Path root = new Path(args[0]);
    int recordNum = 1000;
    if (args.length > 1) {
      recordNum = Integer.parseInt(args[1]);
    }

    HadoopStreams.enforceEmptyDir(conf, root);
    MessageType schema = SchemaParser.parseSchema(
        "message test { "
            + "boolean boolean_field "
            + "int32 int32_field "
            + "int64 int64_field "
            + "string string_field "
            + "double double_field"
            + "} ");
    PrimitiveWriteSupport writeSupport = new PrimitiveWriteSupport(schema);
    SimpleRecordFactory recordFactory = new SimpleRecordFactory(schema);

    Path file = new Path(root, "test.dancemat");
    DancematWriter<Record> writer = new DancematWriter<>(file, writeSupport, 1 * 1024, conf);
    for (int i = 0; i < recordNum; i++) {
      writer.write(
          recordFactory.newRecord()
              .append("boolean_field", i % 2 == 0)
              .append("int32_field", i + 10)
              .append("int64_field", new Long(i))
              .append("double_field", i * 0.1)
              .append("string_field", "sdfjadkfjasf")
      );
    }
    writer.close();
  }
}
