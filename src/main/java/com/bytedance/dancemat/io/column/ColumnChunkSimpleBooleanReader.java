package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.bytes.LittleEndianDataInputStream;
import com.bytedance.dancemat.io.column.Chunk;
import com.bytedance.dancemat.io.file.ColumnChunkReader;
import com.bytedance.dancemat.io.values.PlainValuesReader;

public class ColumnChunkSimpleBooleanReader extends ColumnChunkReader {
  private final PlainValuesReader valuesReader;

  public ColumnChunkSimpleBooleanReader(Chunk chunk) {
    super(chunk);
    this.valuesReader = new PlainValuesReader(new LittleEndianDataInputStream(chunk.getStream()));
  }

  @Override
  public Object read(){
    return valuesReader.readBoolean();
  }
}
