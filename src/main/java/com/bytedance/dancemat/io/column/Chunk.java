package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.bytes.ByteBufferInputStream;
import com.bytedance.dancemat.io.file.ColumnChunkReadStore;
import com.bytedance.dancemat.io.file.DancematFileReader;
import com.bytedance.dancemat.metadata.ColumnChunkMetaData;
import lombok.Getter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Getter
public class Chunk {
  protected final ChunkDescriptor descriptor;
  protected final ByteBufferInputStream stream;

  /**
   *
   * @param descriptor descriptor for the chunk
   * @param buffers ByteBuffers that contain the chunk
   */
  public Chunk(ChunkDescriptor descriptor, List<ByteBuffer> buffers) {
    this.descriptor = descriptor;
    this.stream = ByteBufferInputStream.wrap(buffers);
  }

  /**
   * information needed to read a column chunk
   */
  @Getter
  public static class ChunkDescriptor {

    private final ColumnDescriptor col;
    private final ColumnChunkMetaData metadata;
    private final long fileOffset;
    private final int size;

    /**
     * @param col        column this chunk is part of
     * @param metadata   metadata for the column
     * @param fileOffset offset in the file where this chunk starts
     * @param size       size of the chunk
     */
    public ChunkDescriptor(
        ColumnDescriptor col,
        ColumnChunkMetaData metadata,
        long fileOffset,
        int size) {
      super();
      this.col = col;
      this.metadata = metadata;
      this.fileOffset = fileOffset;
      this.size = size;
    }
  }
}
