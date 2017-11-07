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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects.ToStringHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RangeTracker} for {@link ByteKey ByteKeys} in {@link ByteKeyRange ByteKeyRanges}.
 *
 * @see ByteKey
 * @see ByteKeyRange
 */
public final class ByteKeyRangesTracker implements RangeTracker<ByteKey> {

  private static final Logger LOG = LoggerFactory.getLogger(ByteKeyRangeTracker.class);

  /** Instantiates a new {@link ByteKeyRangesTracker} with the specified range. */
  public static ByteKeyRangesTracker of(List<ByteKeyRange> ranges) {
    return new ByteKeyRangesTracker(new ArrayList<ByteKeyRange>(ranges));
  }

  public synchronized boolean isDone() {
    return done;
  }

  @Override public synchronized ByteKey getStartPosition() {
    return ranges.get(0).getStartKey();
  }

  @Override public synchronized ByteKey getStopPosition() {
    return ranges.get(ranges.size() - 1).getEndKey();
  }

  /** Returns the current range. */
  public synchronized List<ByteKeyRange> getRanges() {
    return ranges;
  }

  @Override public synchronized boolean tryReturnRecordAt(boolean isAtSplitPoint,
      ByteKey recordStart) {
    if (done) {
      return false;
    }

    checkState(!(position == null && !isAtSplitPoint), "The first record must be at a split point");
    checkState(!(recordStart.compareTo(ranges.get(0).getStartKey()) < 0),
        "Trying to return record which is before the start key");
    checkState(!(position != null && recordStart.compareTo(position) < 0),
        "Trying to return record which is before the last-returned record");

    if (position == null) {
      LOG.info("Adjusting range start from {} to {} as position of first returned record",
          getStartPosition(), recordStart);
      setStartPosition(recordStart);
    }
    position = recordStart;

    if (isAtSplitPoint) {
      if (!rangesContainKey(recordStart)) {
        done = true;
        return false;
      }
      ++splitPointsSeen;
    }
    return true;
  }

  @Override public synchronized boolean trySplitAtPosition(ByteKey splitPosition) {
    // Sanity check.
    if (!rangesContainKey(splitPosition)) {
      LOG.warn("{}: Rejecting split request at {} because it is not within the range.", this,
          splitPosition);
      return false;
    }

    // Unstarted.
    if (position == null) {
      LOG.warn("{}: Rejecting split request at {} because no records have been returned.", this,
          splitPosition);
      return false;
    }

    // Started, but not after current position.
    if (splitPosition.compareTo(position) <= 0) {
      LOG.warn("{}: Rejecting split request at {} because it is not after current position {}.",
          this, splitPosition, position);
      return false;
    }

    setEndPosition(splitPosition);
    return true;
  }

  @Override public synchronized double getFractionConsumed() {
    if (position == null) {
      return 0;
    } else if (done) {
      return 1.0;
    } else if (position.compareTo(getStopPosition()) >= 0) {
      return 1.0;
    }
    BigInteger sum = BigInteger.ZERO;
    BigInteger progress = BigInteger.ZERO;
    for (ByteKeyRange range : ranges) {
      BigInteger rangeSize = range.getSize();
      if (position.compareTo(range.getEndKey()) >= 0) {
        progress = progress.add(rangeSize);
      } else if (range.containsKey(position)) {
        BigDecimal rangeSizeDecimal = new BigDecimal(rangeSize);
        BigDecimal fraction = rangeSizeDecimal
            .multiply(new BigDecimal(range.estimateFractionForKey(position)));
        progress = progress.add(fraction.toBigInteger());
      }
      sum = sum.add(rangeSize);
    }

    // Compute the progress (key-start)/(end-start) scaling by 2^64, dividing (which rounds),
    // and then scaling down after the division. This gives ample precision when converted to
    // double.
    BigInteger progressScaled = progress.shiftLeft(64);
    return progressScaled.divide(sum).doubleValue() / Math.pow(2, 64);
  }

  public synchronized long getSplitPointsConsumed() {
    if (position == null) {
      return 0;
    } else if (isDone()) {
      return splitPointsSeen;
    } else {
      // There is a current split point, and it has not finished processing.
      checkState(splitPointsSeen > 0,
          "A started rangeTracker should have seen > 0 split points (is %s)", splitPointsSeen);
      return splitPointsSeen - 1;
    }
  }

  private synchronized void setStartPosition(ByteKey start) {
    for (int i = 0; i < ranges.size(); i++) {
      ByteKeyRange range = ranges.get(i);
      if (range.containsKey(start)) {
        ranges.set(i, range.withStartKey(start));
        return;
      } else {
        ranges.remove(range);
        i--;
      }
    }
  }

  private synchronized void setEndPosition(ByteKey end) {
    for (int i = ranges.size() - 1; i >= 0; i--) {
      ByteKeyRange range = ranges.get(i);
      if (range.containsKey(end)) {
        ranges.set(i, range.withEndKey(end));
        return;
      } else {
        ranges.remove(range);
        i++;
      }
    }
  }

  private synchronized boolean rangesContainKey(ByteKey key) {
    for (ByteKeyRange range : ranges) {
      if (range.containsKey(key)) {
        return true;
      }
    }
    return false;
  }

  ///////////////////////////////////////////////////////////////////////////////
  private List<ByteKeyRange> ranges;
  @Nullable private ByteKey position;
  private long splitPointsSeen;
  private boolean done;

  private ByteKeyRangesTracker(List<ByteKeyRange> ranges) {
    this.ranges = ranges;
    position = null;
    splitPointsSeen = 0L;
    done = false;

    Collections.sort(this.ranges, new Comparator<ByteKeyRange>() {

      @Override public int compare(ByteKeyRange range1, ByteKeyRange range2) {
        return range1.getStartKey().compareTo(range2.getStartKey());
      }
    });
  }

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

  @Override public synchronized String toString() {
    ToStringHelper helper = toStringHelper(ByteKeyRangesTracker.class);
    for (int i = 0; i < ranges.size(); i++) {
      helper.add("range " + i, ranges.get(i));
    }
    return helper.add("position", position).toString();
  }
}
