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

import com.bytedance.dancemat.schema.MessageType;

import java.util.Objects;

/**
 * Abstraction to convert incoming records
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * This will be called once per row group
   * @param recordConsumer the recordConsumer to write to
   */
  public abstract void prepareForWrite(RecordConsumer recordConsumer);

  /**
   * called once per record
   * @param record one record to write to the previously provided record consumer
   */
  public abstract void write(T record);

  /**
   * Called to get a name to identify the WriteSupport object model.
   * If not null, this is added to the file footer metadata.
   * <p>
   * Defining this method will be required in a future API version.
   *
   * @return a String name for file metadata.
   */
  public String getName() {
    return null;
  }

  public MessageType getSchema() {
    return null;
  }



}
