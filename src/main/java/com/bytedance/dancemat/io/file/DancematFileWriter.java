package com.bytedance.dancemat.io.file;

import com.alibaba.fastjson2.JSON;
import com.bytedance.dancemat.api.WriteSupport;
import com.bytedance.dancemat.bytes.BytesUtils;
import com.bytedance.dancemat.io.column.ColumnDescriptor;
import com.bytedance.dancemat.metadata.BlockMetaData;
import com.bytedance.dancemat.metadata.ColumnChunkMetaData;
import com.bytedance.dancemat.metadata.DancematMetadata;
import com.bytedance.dancemat.metadata.FileMetaData;
import com.bytedance.dancemat.schema.MessageType;
import com.bytedance.dancemat.schema.PrimitiveType;
import com.bytedance.dancemat.utils.Properties;
import org.nustaq.serialization.FSTConfiguration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DancematFileWriter {
  public static final String MAGIC_STR = "DANCE";
  public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));

  private final MessageType schema;
  private final DataOutputStream fileOutStream;
  private final long blockSize;

  private List<BlockMetaData> blocks = new ArrayList<>();
  private BlockMetaData currentBlock;
  private long currentRecordCount;
  private long colDataSize;

  private PrimitiveType currentChunkType;         // set in startColumn
  private String currentChunkPath;                // set in startColumn
  private long currentChunkValueCount;            // set in startColumn
  private long currentChunkFirstDataOffset;         // set in startColumn
  private long dataSizeInCurBlock;         // set in startColumn

  private DancematMetadata footer = null;

  public <T> DancematFileWriter(OutputFile file, WriteSupport<T> writeSupport, Properties properties) throws IOException {
    this.schema = writeSupport.getSchema();
    this.fileOutStream = file.getOutStream();
    this.blockSize = properties.getBlockSize();
  }

  public DataOutputStream getFileOutStream() {
    return fileOutStream;
  }

  public void start() throws IOException {
    fileOutStream.write(MAGIC);
  }

  public void startBlock(long recordCount) {
    currentBlock = new BlockMetaData();
    currentRecordCount = recordCount;
  }

  public void startColumn(ColumnDescriptor descriptor,
                          long valueCount,
                          long colDataSize,
                          long firstDataOffset) throws IOException {
    currentChunkPath = descriptor.getPath();
    currentChunkType = descriptor.getPrimitiveType();
    currentChunkFirstDataOffset = firstDataOffset + dataSizeInCurBlock;
    currentChunkValueCount = valueCount;
    this.colDataSize = colDataSize;
    dataSizeInCurBlock += colDataSize;
  }

  public void endColumn() {
    currentBlock.addColumn(new ColumnChunkMetaData (
        currentChunkPath,
        currentChunkType,
        currentChunkFirstDataOffset,
        currentChunkValueCount,
        colDataSize));
    this.currentBlock.setTotalByteSize(currentBlock.getTotalByteSize() + colDataSize);
    this.colDataSize = 0;
  }

  public void endBlock() throws IOException {
    currentBlock.setRowCount(currentRecordCount);
    blocks.add(currentBlock);
    currentBlock = null;
    currentChunkFirstDataOffset = 0;
    dataSizeInCurBlock = 0;
  }

  public void end() throws IOException {
    this.footer = new DancematMetadata(new FileMetaData(schema), blocks);
    serializeFooter(footer);
    fileOutStream.close();
  }

  private void serializeFooter(DancematMetadata footer) throws IOException {
    long footerIndex = fileOutStream.size();
    FSTConfiguration fst = FSTConfiguration.createDefaultConfiguration();
    byte[] bytes = fst.asByteArray(footer);
    fileOutStream.write(bytes);
    BytesUtils.writeIntLittleEndian(fileOutStream, (int) (fileOutStream.size() - footerIndex));
    fileOutStream.write(MAGIC);
  }
}
