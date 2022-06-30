package com.bytedance.dancemat.io.values;

import com.bytedance.dancemat.bytes.LittleEndianDataInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class PlainValuesReader extends ValuesReader {
  private LittleEndianDataInputStream in;

  public static final Charset CHARSET = Charset.forName("UTF-8");

  public PlainValuesReader(InputStream in) {
    this.in = new LittleEndianDataInputStream(in);
  }

  @Override
  public final boolean readBoolean() {
    try {
      return in.readBoolean();
    } catch (IOException e) {
      throw new RuntimeException("could not read boolean", e);
    }
  }

  @Override
  public final String readString() {
    try {
      int len = in.readInt();
      byte[] b = new byte[len];
      in.read(b);
      return new String(b, CHARSET);
    } catch (IOException e) {
      throw new RuntimeException("could not read string", e);
    }
  }

  @Override
  public final int readInteger() {
    try {
      return in.readInt();
    } catch (IOException e) {
      throw new RuntimeException("could not read int", e);
    }
  }

  @Override
  public final long readLong() {
    try {
      long l = in.readLong();
      return l;
    } catch (IOException e) {
      throw new RuntimeException("could not read long", e);
    }
  }

  @Override
  public final double readDouble() {
    try {
      return in.readDouble();
    } catch (IOException e) {
      throw new RuntimeException("could not read double", e);
    }
  }

  @Override
  public byte readByte() {
    try {
      return in.readByte();
    } catch (IOException e) {
      throw new RuntimeException("could not read byte", e);
    }
  }


  @Override
  public void close() throws IOException {
    in.close();
  }
}
