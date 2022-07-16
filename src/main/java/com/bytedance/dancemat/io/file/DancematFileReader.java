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
package com.bytedance.dancemat.io.file;

import com.bytedance.dancemat.Record.RecordReader;
import com.bytedance.dancemat.bytes.BytesUtils;
import com.bytedance.dancemat.io.column.Chunk;
import com.bytedance.dancemat.io.column.ColumnDescriptor;
import com.bytedance.dancemat.io.column.ConsecutiveChunkList;
import com.bytedance.dancemat.metadata.BlockMetaData;
import com.bytedance.dancemat.metadata.ColumnChunkMetaData;
import com.bytedance.dancemat.metadata.DancematMetadata;
import com.bytedance.dancemat.metadata.FileMetaData;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.PrimitiveType;
import com.bytedance.dancemat.schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.nustaq.serialization.FSTConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bytedance.dancemat.io.file.DancematFileWriter.MAGIC;

public class DancematFileReader implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DancematFileReader.class);

  private static FileStatus status(Configuration configuration, Path path) throws IOException {
    return path.getFileSystem(configuration).getFileStatus(path);
  }

  /**
   * @param conf a configuration
   * @param file a file path to open
   * @return a file reader
   * @throws IOException if there is an error while opening the file
   */
  public static DancematFileReader open(Configuration conf, Path file) throws IOException {
    return new DancematFileReader(HadoopInputFile.fromPath(file, conf), conf);
  }

  public static DancematFileReader open(Configuration conf, InputFile file) throws IOException {
    return new DancematFileReader(file, conf);
  }

  private final InputFile file;
  private final SeekableInputStream f;
  private final Configuration conf;
  private final Map<String, Type> paths = new HashMap<>();
  private FileMetaData fileMetaData; // may be null
  private List<BlockMetaData> blocks;
  private DancematMetadata footer;
  private MessageType requestedSchema;

  private int currentBlock = 0;
  private ColumnChunkReadStore recordReader = null;

  public DancematFileReader(InputFile file, Configuration conf) throws IOException {
    this.file = file;
    this.f = file.newStream();
    this.conf = conf;
    this.footer = readFooter(file, f);
    this.fileMetaData = footer.getFileMetaData();
    this.blocks = footer.getBlocks();
  }

  private static final DancematMetadata readFooter(InputFile file, SeekableInputStream f) throws IOException {
    long fileLen = file.getLength();
    String filePath = file.toString();
    LOG.debug("File length {}", fileLen);
    int FOOTER_LENGTH_SIZE = 4;
    if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer + footerIndex + MAGIC
      throw new RuntimeException(filePath + " is not a Dancemat file (too small length: " + fileLen + ")");
    }
    long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
    LOG.debug("reading footer index at {}", footerLengthIndex);

    f.seek(footerLengthIndex);
    int footerLength = BytesUtils.readIntLittleEndian(f);
    byte[] magic = new byte[MAGIC.length];
    f.readFully(magic);
    if (!Arrays.equals(MAGIC, magic)) {
      throw new RuntimeException(filePath + " is not a Dancemat file. expected magic number at tail " + Arrays.toString(MAGIC) + " but found " + Arrays.toString(magic));
    }
    long footerIndex = footerLengthIndex - footerLength;
    LOG.debug("read footer length: {}, footer index: {}", footerLength, footerIndex);
    if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
      throw new RuntimeException("corrupted file: the footer index is not within the file: " + footerIndex);
    }
    f.seek(footerIndex);

    byte[] bytes = new byte[footerLength];
    f.readFully(bytes);
    FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();
    DancematMetadata dancematMetadata = (DancematMetadata) fst.asObject(bytes);
    return dancematMetadata;
  }

  public DancematMetadata getFooter() {
    return footer;
  }

  /**
   * @return the path for this file
   * @deprecated will be removed in 2.0.0; use {@link #getFile()} instead
   */
  @Deprecated
  public Path getPath() {
    return new Path(file.toString());
  }

  public String getFile() {
    return file.toString();
  }

  public List<BlockMetaData> getRowGroups() {
    return blocks;
  }

  public long getRecordCount() {
    long total = 0;
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    return total;
  }

  @Override
  public void close() throws IOException {

  }

  public void setRequestedSchema(MessageType projection) {
    paths.clear();
    for (Type type : projection.getFields()) {
      paths.put(type.getName(), type);
    }
    requestedSchema = projection;
  }

  /**
   * Reads all the columns requested from the row group at the current file position.
   *
   * @return the PageReadStore which can provide PageReaders for each column.
   * @throws IOException if an error occurs while reading
   */
  public RecordReader readNextRowGroup() throws IOException {
    if (currentBlock == blocks.size()) {
      return null;
    }
    BlockMetaData block = blocks.get(currentBlock);
    if (block.getRowCount() == 0) {
      throw new RuntimeException("Illegal row group of 0 rows");
    }

    this.recordReader = new ColumnChunkReadStore(block.getRowCount(), requestedSchema);
    List<ConsecutiveChunkList> allChunks = new ArrayList<ConsecutiveChunkList>();
    ConsecutiveChunkList currentChunks = null;
    for (ColumnChunkMetaData mc : block.getColumns()) {
      if (paths.containsKey(mc.getPath())) {
        long startingPos = mc.getStartingPos();
        // first chunk or not consecutive => new list
        if (currentChunks == null || currentChunks.endPos() != startingPos) {
          currentChunks = new ConsecutiveChunkList(startingPos);
          allChunks.add(currentChunks);
        }
        currentChunks.addChunk(new Chunk.ChunkDescriptor(new ColumnDescriptor(mc.getType()), mc, startingPos, (int) mc.getTotalSize()));
      }
    }

    for (ConsecutiveChunkList consecutiveChunks : allChunks) {
      final List<Chunk> chunks = consecutiveChunks.readAll(f);
      for (Chunk chunk : chunks) {
        recordReader.addColumn(chunk.getDescriptor().getCol(), chunk);
      }
    }
    advanceToNextBlock();
    return recordReader;
  }

  private boolean advanceToNextBlock() {
    if (currentBlock == blocks.size()) {
      return false;
    }
    ++currentBlock;
    return true;
  }


}
