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
package com.bytedance.dancemat.api;


import com.bytedance.dancemat.io.column.ColumnWriter;
import com.bytedance.dancemat.schema.Type;

/**
 *
 * Abstraction for writing records
 * It decouples the striping algorithm from the actual record model
 * example:
 * <pre>
 * startMessage()
 *  startField("A", 0)
 *   addValue(1)
 *   addValue(2)
 *  endField("A", 0)
 *  startField("B", 1)
 *   startGroup()
 *    startField("C", 0)
 *     addValue(3)
 *    endField("C", 0)
 *   endGroup()
 *  endField("B", 1)
 * endMessage()
 * </pre>
 *
 * would produce the following message:
 * <pre>
 * {
 *   A: [1, 2]
 *   B: {
 *     C: 3
 *   }
 * }
 * </pre>
 */
abstract public class RecordConsumer {

  /**
   * start a new record
   */
  abstract public void startMessage();

  /**
   * end of a record
   */
  abstract public void endMessage();

  /**
   * start of a field in a group or message
   * if the field is repeated the field is started only once and all values added in between start and end
   * @param field name of the field
   * @param index of the field in the group or message
   */
  abstract public void startField(String field, int index);

  /**
   * end of a field in a group or message
   * @param field name of the field
   * @param index of the field in the group or message
   */
  abstract public void endField(String field, int index);

  /**
   * add an int value in the current field
   * @param value an int value
   */
  abstract public void addInteger(int value);

  /**
   * add a long value in the current field
   * @param value a long value
   */
  abstract public void addLong(long value);

  /**
   * add a boolean value in the current field
   * @param value a boolean value
   */
  abstract public void addBoolean(boolean value);

  /**
   * add a binary value in the current field
   * @param value a binary value
   */
  abstract public void addString(String value);

  /**
   * add a double value in the current field
   * @param value a double value
   */
  abstract public void addDouble(double value);

  public long bytesWritten(){
    return 0L;
  }

  abstract public ColumnWriter[] getColumnWriters();

  abstract public long getValueCount();

  abstract public long getFirstDataOffset();

  /**
   * NoOps by default
   * Subclass class can implement its own flushing logic
   */
  public void flush() {
  }

}
