package com.bytedance.dancemat.bytes;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CapacityOutputStream extends OutputStream {
  private static final int DEFAULT_ALLOCATE_SIZE = 8 * 1024 * 1024;

  private long bytesUsed = 0;
  private int bytesAllocated = 0;
  private final List<ByteBuffer> slabs = new ArrayList<ByteBuffer>();
  private ByteBuffer currentSlab;
  private int currentSlabIndex;
  private OutputStream fileOutStream;
  private final ByteBufferAllocator allocator;

  public CapacityOutputStream(OutputStream os) {
    this.fileOutStream = os;
    this.allocator = HeapByteBufferAllocator.getInstance();
    addSlab();
  }

  @Override
  public void write(int b) throws IOException {
    if (!currentSlab.hasRemaining()) {
      addSlab();
    }
    currentSlab.put(currentSlabIndex, (byte) b);
    currentSlabIndex += 1;
    currentSlab.position(currentSlabIndex);
    bytesUsed += 1;
//    fileOutStream.write(b);
  }

  @Override
  public void write(byte b[]) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException(
          String.format("Given byte array of size %d, with requested length(%d) and offset(%d)", b.length, len, off));
    }
    if (len >= currentSlab.remaining()) {
      final int length1 = currentSlab.remaining();
      currentSlab.put(b, off, length1);
      bytesUsed += length1;
      currentSlabIndex += length1;
      final int length2 = len - length1;
      addSlab();
      currentSlab.put(b, off + length1, length2);
      currentSlabIndex = length2;
      bytesUsed += length2;
    } else {
      currentSlab.put(b, off, len);
      currentSlabIndex += len;
      bytesUsed += len;
    }
//    fileOutStream.write(b, off, len);
  }

  private void addSlab() {
    int nextSlabSize = DEFAULT_ALLOCATE_SIZE;

    this.currentSlab = allocator.allocate(nextSlabSize);
    this.slabs.add(currentSlab);
    this.bytesAllocated += nextSlabSize;
    this.currentSlabIndex = 0;
  }

  public long getBytesUsed() {
    return bytesUsed;
  }

  @Override
  public void close() throws IOException {
    for (ByteBuffer slab : slabs) {
      fileOutStream.write(slab.array(), 0, slab.position());
      allocator.release(slab);
    }
//    this.fileOutStream.close();
  }
}
