/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.bytedance.dancemat.io.file;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class HadoopSeekableInputStream extends SeekableInputStream {
  private final FSDataInputStream stream;
  private final int BUFFER_SIZE = 8 * 1024 * 1024;

  public HadoopSeekableInputStream(FSDataInputStream stream) {
    this.stream = stream;
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    stream.readFully(bytes);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(bytes);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return stream.read(buf);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    int bufferSize = BUFFER_SIZE;
    byte[] buffer = new byte[Math.min(buf.remaining(), bufferSize)];
    while (buf.hasRemaining()) {
      int readCount = stream.read(buffer);
      buf.put(buffer, 0, readCount);
      if (readCount == -1) {
        // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
        // that has more remaining than the amount of data in the stream.
        throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
      }
    }
  }
}
