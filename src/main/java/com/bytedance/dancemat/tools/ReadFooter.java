package com.bytedance.dancemat.tools;

import com.bytedance.dancemat.io.file.DancematFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ReadFooter {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    DancematFileReader reader = DancematFileReader.open(conf, new Path(args[0]));
    System.out.println(reader.getFooter());
  }
}
