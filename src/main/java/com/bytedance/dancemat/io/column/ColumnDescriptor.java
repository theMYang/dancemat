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
package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.schema.PrimitiveType;

import java.util.Arrays;

/**
 * Describes a column's type as well as its position in its containing schema.
 */
public class ColumnDescriptor implements Comparable<ColumnDescriptor> {

  private final String path;
  private final PrimitiveType type;

  /**
   * @param path the path to the leaf field in the schema
   * @param type the type of the field
   */
  public ColumnDescriptor(PrimitiveType type) {
    this.path = type.getName();
    this.type = type;
  }

  /**
   * @return the path to the leaf field in the schema
   */
  public String getPath() {
    return path;
  }

  /**
   * @return the primitive type object of the column
   */
  public PrimitiveType getPrimitiveType() {
    return type;
  }

  @Override
  public int hashCode() {
    return path.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (!(other instanceof ColumnDescriptor))
      return false;
    ColumnDescriptor descriptor = (ColumnDescriptor) other;
    return path.equals(descriptor.path);
  }

  @Override
  public int compareTo(ColumnDescriptor o) {
    return path.compareTo(o.path);
  }

  @Override
  public String toString() {
    return path + " " + type;
  }
}
