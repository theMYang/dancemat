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
package com.bytedance.dancemat.metadata;

import com.bytedance.dancemat.schema.PrimitiveType;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Column meta data for a block stored in the file footer and passed in the InputSplit
 */
@Getter
public class ColumnChunkMetaData implements Serializable {

  private final PrimitiveType type;
  private final String path;
  private final long firstDataPage;
  private final long valueCount;
  private final long totalSize;

  /**
   * @param path column identifier
   * @param type type of the column
   * @param firstDataPage
   * @param valueCount
   * @param totalSize
   */
  public ColumnChunkMetaData(
      String path,
      PrimitiveType type,
      long firstDataPage,
      long valueCount,
      long totalSize) {
    this.path = path;
    this.type = type;
    this.firstDataPage = firstDataPage;
    this.valueCount = valueCount;
    this.totalSize = totalSize;
  }

  /**
   * @return the offset of the first byte in the chunk
   */
  public long getStartingPos() {
    long firstDataPageOffset = getFirstDataPageOffset();
    return firstDataPageOffset;
  }

  public PrimitiveType getPrimitiveType() {
    return type;
  }

  /**
   * @return start of the column data offset
   */
  public long getFirstDataPageOffset() {
    return firstDataPage;
  }

  public long getValueCount() {
    return valueCount;
  }

  public long getTotalSize() {
    return totalSize;
  }

  @Override
  public String toString() {
    return "\nColumnChunkMetaData{" +
        "\ntype=" + type +
        "\npath='" + path + '\'' +
        "\nfirstDataPage=" + firstDataPage +
        "\nvalueCount=" + valueCount +
        "\ntotalSize=" + totalSize +
        "\n}";
  }
}
