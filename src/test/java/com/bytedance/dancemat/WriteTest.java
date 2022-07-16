package com.bytedance.dancemat;

import com.bytedance.dancemat.api.ReadSupport;
import com.bytedance.dancemat.data.Record;
import com.bytedance.dancemat.data.SimpleRecordFactory;
import com.bytedance.dancemat.io.PrimitiveWriteSupport;
import com.bytedance.dancemat.io.file.DancematFileReader;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.SchemaParser;
import com.bytedance.dancemat.utils.TestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class WriteTest {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestDancemat/");
    TestUtils.enforceEmptyDir(conf, root);
    MessageType schema = SchemaParser.parseSchema(
        "message test { "
            + "boolean boolean_field "
            + "int32 int32_field "
            + "int64 int64_field "
            + "string string_field "
            + "double double_field"
            + "} ");
//    MessageType schema = SchemaParser.parseSchema(
//        "message test { "
//            + "binary binary_field; "
//            + "int32 int32_field; "
//            + "int64 int64_field; "
//            + "boolean boolean_field; "
//            + "double double_field; "
//            + "} ");
    PrimitiveWriteSupport writeSupport = new PrimitiveWriteSupport(schema);
    SimpleRecordFactory recordFactory = new SimpleRecordFactory(schema);

    Path file = new Path(root, "test.dancemat");
    DancematWriter<Record> writer = new DancematWriter<>(file, writeSupport, 1 * 1024, conf);
    for (int i = 0; i < 100; i++) {
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

    String requestedSchema = "message test { "
        + "boolean boolean_field "
        + "int64 int64_field"
        + "} ";
//    String requestedSchema = "message test { "
//        + "boolean boolean_field "
//        + "int32 int32_field "
//        + "int64 int64_field "
//        + "double double_field"
//        + "} ";
    conf.set(ReadSupport.DANCEMAT_READ_SCHEMA, requestedSchema);
    DancematReader<Record> reader = DancematReader.builder(new PrimitiveReadSupport(), file).withConf(conf).build();
    for (int i = 0; i < 100; i++) {
      Record record = reader.read();
      System.out.println(record);
    }
  }
}
