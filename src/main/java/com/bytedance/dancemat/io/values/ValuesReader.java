package com.bytedance.dancemat.io.values;

import java.io.IOException;

public abstract class ValuesReader {
  /**
   * @param value the value to encode
   */
  public byte readByte() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public boolean readBoolean() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public String readString() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public int readInteger() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public long readLong() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * @param v the value to encode
   */
  public double readDouble() {
    throw new UnsupportedOperationException(getClass().getName());
  }

  /**
   * Called to close the values writer. Any output stream is closed and can no longer be used.
   * All resources are released.
   */
  public void close() throws IOException {
  }
}
