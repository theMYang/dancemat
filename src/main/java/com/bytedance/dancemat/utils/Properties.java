package com.bytedance.dancemat.utils;

import lombok.Builder;
import lombok.Getter;

@Builder
public class Properties {
  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;

  private int blockSize;

  public int getBlockSize() {
    return blockSize == 0? DEFAULT_BLOCK_SIZE : blockSize;
  }
}
