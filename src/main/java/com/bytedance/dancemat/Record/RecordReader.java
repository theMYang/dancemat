package com.bytedance.dancemat.Record;

/**
 * used to read reassembled records
 *
 * @param <T> the type of the materialized record
 */
public abstract class RecordReader<T> {

  /**
   * Reads one record and returns it.
   * @return the materialized record
   */
  public abstract T read();

  public abstract long getRowCount();

  /**
   * Returns whether the current record should be skipped (dropped)
   * Will be called *after* read()
   * @return true if the current record should be skipped
   */
  public boolean shouldSkipCurrentRecord() {
    return false;
  }
}
