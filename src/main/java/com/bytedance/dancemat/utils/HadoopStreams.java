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

package com.bytedance.dancemat.utils;

import com.bytedance.dancemat.io.file.HadoopSeekableInputStream;
import com.bytedance.dancemat.io.file.SeekableInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 */
public class HadoopStreams {
  /**
   * Wraps a {@link FSDataInputStream} in a {@link SeekableInputStream}
   * implementation for Parquet readers.
   *
   * @param stream a Hadoop FSDataInputStream
   * @return a SeekableInputStream
   */
  public static SeekableInputStream wrap(FSDataInputStream stream) {
    Objects.requireNonNull(stream, "Cannot wrap a null input stream");
    return new HadoopSeekableInputStream(stream);
  }

  public static void enforceEmptyDir(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("can not delete path " + path);
      }
    }
    if (!fs.mkdirs(path)) {
      throw new IOException("can not create path " + path);
    }
  }
}
