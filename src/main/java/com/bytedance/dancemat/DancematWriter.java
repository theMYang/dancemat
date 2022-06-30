package com.bytedance.dancemat;

import com.bytedance.dancemat.api.WriteSupport;
import com.bytedance.dancemat.io.file.DancematFileWriter;
import com.bytedance.dancemat.io.file.HadoopOutputFile;
import com.bytedance.dancemat.io.InternalRecordWriter;
import com.bytedance.dancemat.utils.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;

public class DancematWriter<T> implements Closeable {

  private final InternalRecordWriter<T> writer;

  public DancematWriter(Path file, WriteSupport<T> writeSupport, int blockSize, Configuration conf) throws IOException {
    Properties properties = Properties.builder().blockSize(blockSize).build();
    DancematFileWriter fileWriter = new DancematFileWriter(HadoopOutputFile.fromPath(file, conf), writeSupport, properties);
    fileWriter.start();

    this.writer = new InternalRecordWriter<T>(fileWriter, writeSupport, writeSupport.getSchema(), properties);
  }

  public void write(T object) throws IOException {
    try {
      writer.write(object);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }
}
