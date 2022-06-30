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

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Log4j
public class HadoopOutputFile implements OutputFile {
  private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;

  private static final Set<String> BLOCK_FS_SCHEMES = new HashSet<String>();
  static {
    BLOCK_FS_SCHEMES.add("hdfs");
  }

  // visible for testing
  public static Set<String> getBlockFileSystems() {
    return BLOCK_FS_SCHEMES;
  }

  private static boolean supportsBlockSize(FileSystem fs) {
    return BLOCK_FS_SCHEMES.contains(fs.getUri().getScheme());
  }

  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;

  public static HadoopOutputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopOutputFile(fs, fs.makeQualified(path), conf);
  }

  private HadoopOutputFile(FileSystem fs, Path path, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
  }

  @Override
  public DataOutputStream getOutStream() {
    try {
      return fs.create(path);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create output stream for " + path);
    }
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public boolean supportsBlockSize() {
    return supportsBlockSize(fs);
  }

  @Override
  public long defaultBlockSize() {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
