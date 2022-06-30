/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bytedance.dancemat.io;

import com.bytedance.dancemat.Record.RecordReader;
import com.bytedance.dancemat.api.ReadSupport;
import com.bytedance.dancemat.io.column.ColumnIOFactory;
import com.bytedance.dancemat.io.file.DancematFileReader;
import com.bytedance.dancemat.metadata.FileMetaData;
import com.bytedance.dancemat.schema.MessageType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.String.format;

public class InternalRecordReader<T> {
  private static final Logger LOG = LoggerFactory.getLogger(InternalRecordReader.class);

  private DancematFileReader reader;
  private MessageType requestedSchema;
  private MessageType fileSchema;
  private int columnCount;
  private long total;
  private long current = 0;
  private int currentBlock = -1;
  private long totalCountLoadedSoFar = 0;
  private final ReadSupport<T> readSupport;
  private ColumnIOFactory columnIOFactory = null;
  private T currentValue;
  private RecordReader<T> recordReader;

  public InternalRecordReader(ReadSupport<T> readSupport) {
    this.readSupport = readSupport;
  }

  public void initialize(DancematFileReader reader, Configuration conf) {
    this.reader = reader;
    FileMetaData fileMetadata = reader.getFooter().getFileMetaData();
    this.fileSchema = fileMetadata.getSchema();
    ReadSupport.ReadContext readContext = readSupport.init(conf, fileSchema);
    this.columnIOFactory = new ColumnIOFactory();
    this.requestedSchema = readContext.getRequestedSchema();
    this.columnCount = requestedSchema.getFieldCount();
    this.total = reader.getRecordCount();
    reader.setRequestedSchema(requestedSchema);
  }

  public boolean nextKeyValue() throws IOException {
    boolean recordFound = false;

    while (!recordFound) {
      // no more records left
      if (current >= total) { return false; }

      try {
        checkRead();
        current ++;

        currentValue = recordReader.read();
        if (recordReader.shouldSkipCurrentRecord()) {
          continue;
        }

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = totalCountLoadedSoFar;
          LOG.debug("filtered record reader reached end of block");
          continue;
        }

        recordFound = true;

        LOG.debug("read value: {}", currentValue);
      } catch (RuntimeException e) {
        throw new RuntimeException(format("Can not read value at %d in block %d in file %s", current, currentBlock, reader.getPath()), e);
      }
    }
    return true;
  }

  private void checkRead() throws IOException {
    if (current == totalCountLoadedSoFar) {
      if (current != 0) {
      }

      recordReader = reader.readNextRowGroup();
      if (recordReader == null) {
        throw new IOException("expecting more rows but reached last block. Read " + current + " out of " + total);
      }
      totalCountLoadedSoFar += recordReader.getRowCount();
      ++ currentBlock;
    }
  }

  public T getCurrentValue() throws IOException {
    return currentValue;
  }
}
