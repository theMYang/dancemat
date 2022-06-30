package com.bytedance.dancemat;

import com.bytedance.dancemat.api.ReadSupport;
import com.bytedance.dancemat.io.InternalRecordReader;
import com.bytedance.dancemat.io.file.DancematFileReader;
import com.bytedance.dancemat.io.file.HadoopInputFile;
import com.bytedance.dancemat.io.file.InputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class DancematReader<T> implements Closeable {
  private final ReadSupport<T> readSupport;
  private final Iterator<InputFile> filesIterator;
  private final Configuration conf;

  private T currentValue;
  private InternalRecordReader<T> reader;

  private DancematReader(List<InputFile> files,
                         Configuration conf,
                         ReadSupport<T> readSupport) throws IOException {
    this.readSupport = readSupport;
    this.conf = conf;
    this.filesIterator = files.iterator();
  }

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<>(readSupport, path);
  }

  public T read() throws IOException {
    if (reader != null && reader.nextKeyValue()) {
      return reader.getCurrentValue();
    } else {
      initReader();
      return reader == null ? null : read();
    }
  }

  private void initReader() throws IOException {
    if (reader != null) {
//      reader.close();
      reader = null;
    }

    if (filesIterator.hasNext()) {
      InputFile file = filesIterator.next();

      DancematFileReader fileReader = DancematFileReader.open(conf, file);

      reader = new InternalRecordReader<>(readSupport);

      reader.initialize(fileReader, conf);
    }
  }


  @Override
  public void close() throws IOException {
    if (reader != null) {
//      reader.close();
    }
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final InputFile file;
    private final Path path;
    protected Configuration conf;

    @Deprecated
    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = Objects.requireNonNull(readSupport, "readSupport");
      this.file = null;
      this.path = Objects.requireNonNull(path, "path");
      this.conf = new Configuration();
    }

    public Builder<T> set(String key, String value) {
      conf.set(key, value);
      return this;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    protected ReadSupport<T> getReadSupport() {
      return readSupport;
    }

    public DancematReader<T> build() throws IOException {
      if (path != null) {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus stat = fs.getFileStatus(path);

        if (stat.isFile()) {
          return new DancematReader(
              Collections.singletonList(HadoopInputFile.fromStatus(stat, conf)),
              conf,
              getReadSupport());
        } else {
          List<InputFile> files = new ArrayList<>();
          for (FileStatus fileStatus : fs.listStatus(path)) {
            files.add(HadoopInputFile.fromStatus(fileStatus, conf));
          }
          return new DancematReader<T>(files, conf, getReadSupport());
        }
      } else {
        return new DancematReader<>(Collections.singletonList(file), conf, getReadSupport());
      }
    }
  }
}
