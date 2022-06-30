package com.bytedance.dancemat.io.values;

import com.bytedance.dancemat.bytes.ByteBufferAllocator;
import com.bytedance.dancemat.bytes.CapacityOutputStream;
import com.bytedance.dancemat.bytes.LittleEndianDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class PlainValuesWriter extends ValuesWriter {
  private LittleEndianDataOutputStream out;
  private CapacityOutputStream capacityOutputStream;

  public static final Charset CHARSET = Charset.forName("UTF-8");

  public PlainValuesWriter(OutputStream os) {
    this.capacityOutputStream = new CapacityOutputStream(os);
    out = new LittleEndianDataOutputStream(capacityOutputStream);
  }

  @Override
  public final void writeBoolean(boolean v) {
    try {
      out.writeBoolean(v);
    } catch (IOException e) {
      throw new RuntimeException("could not write boolean", e);
    }
  }

  @Override
  public final void writeString(String v) {
    try {
      byte[] bytes = v.getBytes(CHARSET);
      out.writeInt(bytes.length);
      out.write(bytes);
    } catch (IOException e) {
      throw new RuntimeException("could not write string", e);
    }
  }

  @Override
  public final void writeInteger(int v) {
    try {
      out.writeInt(v);
    } catch (IOException e) {
      throw new RuntimeException("could not write int", e);
    }
  }

  @Override
  public final void writeLong(long v) {
    try {
      out.writeLong(v);
    } catch (IOException e) {
      throw new RuntimeException("could not write long", e);
    }
  }

  @Override
  public final void writeDouble(double v) {
    try {
      out.writeDouble(v);
    } catch (IOException e) {
      throw new RuntimeException("could not write double", e);
    }
  }

  @Override
  public void writeByte(int value) {
    try {
      out.write(value);
    } catch (IOException e) {
      throw new RuntimeException("could not write byte", e);
    }
  }


  @Override
  public void close() throws IOException {
    out.close();
  }

  @Override
  public long getBufferedSize() {
    return capacityOutputStream.getBytesUsed();
  }

}
