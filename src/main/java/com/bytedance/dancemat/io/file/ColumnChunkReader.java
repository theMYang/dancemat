package com.bytedance.dancemat.io.file;

import com.bytedance.dancemat.bytes.LittleEndianDataInputStream;
import com.bytedance.dancemat.io.column.Chunk;

public abstract class ColumnChunkReader {
  protected final Chunk chunk;

  public ColumnChunkReader(Chunk chunk) {
    this.chunk = chunk;
  }

  public abstract Object read();
}
