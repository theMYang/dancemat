package com.bytedance.dancemat.io.values;

import java.io.IOException;

public abstract class ValuesWriter {
  /**
   * ( &gt; {@link #getBufferedSize} )
   * @return the allocated size of the buffer
   */
  abstract public long getBufferedSize();

  /**
   * @param value the value to encode
   */
  public void writeByte(int value) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeBoolean(boolean v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeString(String v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeInteger(int v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeLong(long v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public void writeDouble(double v) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Called to close the values writer. Any output stream is closed and can no longer be used.
   * All resources are released.
   */
  public void close() throws IOException {
  }
}
