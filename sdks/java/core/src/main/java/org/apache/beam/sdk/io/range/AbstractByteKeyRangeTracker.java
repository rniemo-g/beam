/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.range;

import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link RangeTracker} for {@link ByteKey ByteKeys} in
 * {@link ByteKeyRange ByteKeyRanges}.
 *
 * @see ByteKey
 * @see ByteKeyRange
 */
public abstract class AbstractByteKeyRangeTracker implements RangeTracker<ByteKey> {
  private static final Logger LOG = LoggerFactory.getLogger(ByteKeyRangeTracker.class);

  public synchronized boolean isDone() {
    return done;
  }

  @Override
  public synchronized boolean tryReturnRecordAt(boolean isAtSplitPoint, ByteKey recordStart) {
    if (done) {
      return false;
    }

    checkState(!(position == null && !isAtSplitPoint), "The first record must be at a split point");
    checkState(!(recordStart.compareTo(getStartPosition()) < 0),
        "Trying to return record which is before the start key");
    checkState(!(position != null && recordStart.compareTo(position) < 0),
        "Trying to return record which is before the last-returned record");

    if (position == null) {
      LOG.info(
          "Adjusting range start from {} to {} as position of first returned record",
          getStartPosition(),
          recordStart);
      setStartPosition(recordStart);
    }
    position = recordStart;

    if (isAtSplitPoint) {
      if (!containsKey(recordStart)) {
        done = true;
        return false;
      }
      ++splitPointsSeen;
    }
    return true;
  }

  @Override
  public synchronized boolean trySplitAtPosition(ByteKey splitPosition) {
    // Sanity check.
    if (!containsKey(splitPosition)) {
      LOG.warn(
          "{}: Rejecting split request at {} because it is not within the range.",
          this,
          splitPosition);
      return false;
    }

    // Unstarted.
    if (position == null) {
      LOG.warn(
          "{}: Rejecting split request at {} because no records have been returned.",
          this,
          splitPosition);
      return false;
    }

    // Started, but not after current position.
    if (splitPosition.compareTo(position) <= 0) {
      LOG.warn(
          "{}: Rejecting split request at {} because it is not after current position {}.",
          this,
          splitPosition,
          position);
      return false;
    }

    setEndPosition(splitPosition);
    return true;
  }

  /**
   * Sets the end position of the range tracker. After this is called, any {@link ByteKey} after
   * {@code end} is not considered in this range tracker.
   */
  public abstract void setEndPosition(ByteKey end);

  /**
   * Sets the start position of the range tracker. After this is called, any {@link ByteKey} before
   * {@code start} is not considered in this range tracker.
   */
  public abstract void setStartPosition(ByteKey start);

  /**
   * Determines whether {@code key} is in this range tracker or not.
   *
   * @return True if {@code key} is in this tracker, false otherwise.
   */
  public abstract boolean containsKey(ByteKey key);

  public synchronized long getSplitPointsConsumed() {
    if (position == null) {
      return 0;
    } else if (isDone()) {
      return splitPointsSeen;
    } else {
      // There is a current split point, and it has not finished processing.
      checkState(
          splitPointsSeen > 0,
          "A started rangeTracker should have seen > 0 split points (is %s)",
          splitPointsSeen);
      return splitPointsSeen - 1;
    }
  }

  ///////////////////////////////////////////////////////////////////////////////
  @Nullable protected ByteKey position;
  protected long splitPointsSeen;
  protected boolean done;


  /**
   * Marks this range tracker as being done. Specifically, this will mark the current split point,
   * if one exists, as being finished.
   *
   * <p>Always returns false, so that it can be used in an implementation of
   * {@link BoundedReader#start()} or {@link BoundedReader#advance()} as follows:
   *
   * <pre> {@code
   * public boolean start() {
   *   return startImpl() && rangeTracker.tryReturnRecordAt(isAtSplitPoint, position)
   *       || rangeTracker.markDone();
   * }} </pre>
   */
  public synchronized boolean markDone() {
    done = true;
    return false;
  }

}
