package com.bytedance.dancemat.io.column;

import com.bytedance.dancemat.bytes.ByteBufferInputStream;
import com.bytedance.dancemat.io.file.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ConsecutiveChunkList {

  private static final int ALLOCATION_SIZE_DEFAULT = 8388608; // 8MB

  private final long offset;
  private int length;
  private final List<Chunk.ChunkDescriptor> chunks = new ArrayList<Chunk.ChunkDescriptor>();

  /**
   * @param offset where the first chunk starts
   */
  public ConsecutiveChunkList(long offset) {
    this.offset = offset;
  }

  /**
   * adds a chunk to the list.
   * It must be consecutive to the previous chunk
   *
   * @param descriptor a chunk descriptor
   */
  public void addChunk(Chunk.ChunkDescriptor descriptor) {
    chunks.add(descriptor);
    length += descriptor.getSize();
  }

  /**
   * @param f file to read the chunks from
   * @return the chunks
   * @throws IOException if there is an error while reading from the stream
   */
  public List<Chunk> readAll(SeekableInputStream f) throws IOException {
    List<Chunk> result = new ArrayList<Chunk>(chunks.size());
    f.seek(offset);

    int fullAllocations = length / ALLOCATION_SIZE_DEFAULT;
    int lastAllocationSize = length % ALLOCATION_SIZE_DEFAULT;

    int numAllocations = fullAllocations + (lastAllocationSize > 0 ? 1 : 0);
    List<ByteBuffer> buffers = new ArrayList<>(numAllocations);

    for (int i = 0; i < fullAllocations; i += 1) {
      buffers.add(ByteBuffer.allocate(ALLOCATION_SIZE_DEFAULT));
    }

    if (lastAllocationSize > 0) {
      buffers.add(ByteBuffer.allocate(lastAllocationSize));
    }

    for (ByteBuffer buffer : buffers) {
      f.readFully(buffer);
      buffer.flip();
    }

    ByteBufferInputStream stream = ByteBufferInputStream.wrap(buffers);
    for (int i = 0; i < chunks.size(); i++) {
      Chunk.ChunkDescriptor descriptor = chunks.get(i);
      result.add(new Chunk(descriptor, stream.sliceBuffers(descriptor.getSize())));
    }
    return result;
  }

  /**
   * @return the position following the last byte of these chunks
   */
  public long endPos() {
    return offset + length;
  }

}
