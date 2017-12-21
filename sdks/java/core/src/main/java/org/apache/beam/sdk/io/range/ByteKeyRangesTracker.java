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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.MoreObjects.ToStringHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A {@link RangeTracker} for {@link ByteKey ByteKeys} in multiple
 * {@link ByteKeyRange ByteKeyRanges}. Represents multiple {@link ByteKeyRange ByteKeyRanges} as
 * a single contiguous range, handling edge cases at the ends of each range.
 *
 * @see ByteKey
 * @see ByteKeyRange
 */
public final class ByteKeyRangesTracker extends AbstractByteKeyRangeTracker {

  /** Instantiates a new {@link ByteKeyRangesTracker} with the specified range. */
  public static ByteKeyRangesTracker of(List<ByteKeyRange> ranges) {
    return new ByteKeyRangesTracker(new ArrayList<ByteKeyRange>(ranges));
  }

  @Override
  public synchronized ByteKey getStartPosition() {
    return ranges.get(0).getStartKey();
  }

  @Override
  public synchronized ByteKey getStopPosition() {
    return ranges.get(ranges.size() - 1).getEndKey();
  }

  @Override
  public synchronized void setEndPosition(ByteKey end) {
    for (int i = ranges.size() - 1; i >= 0; i--) {
      ByteKeyRange range = ranges.get(i);
      if (range.containsKey(end)) {
        ranges.set(i, range.withEndKey(end));
        calculateRangesSize();
        return;
      } else {
        ranges.remove(range);
      }
    }
    calculateRangesSize();
  }

  @Override
  public synchronized void setStartPosition(ByteKey start) {
    for (int i = 0; i < ranges.size(); i++) {
      ByteKeyRange range = ranges.get(i);
      if (range.containsKey(start)) {
        ranges.set(i, range.withStartKey(start));
        calculateRangesSize();
        return;
      } else {
        ranges.remove(range);
        i--;
      }
    }
    calculateRangesSize();
  }

  @Override
  public boolean containsKey(ByteKey key) {
    for (ByteKeyRange range : ranges) {
      if (range.containsKey(key)) {
        return true;
      }
    }
    return false;
  }

  /** Returns the current list of ranges. */
  public synchronized List<ByteKeyRange> getRanges() {
    return ranges;
  }

  @Override
  public synchronized double getFractionConsumed() {
    if (position == null) {
      return 0;
    } else if (done) {
      return 1.0;
    } else if (position.compareTo(getStopPosition()) >= 0) {
      return 1.0;
    }
    BigInteger progress = BigInteger.ZERO;
    for (ByteKeyRange range : ranges) {
      BigInteger rangeSize = range.getSize();
      if (position.compareTo(range.getEndKey()) >= 0) {
        progress = progress.add(rangeSize);
      } else if (range.containsKey(position)) {
        BigInteger positionValue = new BigInteger(position.getBytes());
        BigInteger rangeStartValue = new BigInteger(range.getStartKey().getBytes());
        progress = progress.add(positionValue.subtract(rangeStartValue));
      } else {
        break;
      }
    }

    // Compute the progress (key-start)/(end-start) scaling by 2^64, dividing (which rounds),
    // and then scaling down after the division. This gives ample precision when converted to
    // double.
    BigInteger progressScaled = progress.shiftLeft(64);
    return progressScaled.divide(rangesSize).doubleValue() / Math.pow(2, 64);
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

  public synchronized ByteKey interpolateKey(double fraction) {
    checkArgument(
        fraction >= 0.0 && fraction < 1.0, "Fraction %s must be in the range [0, 1)", fraction);
    BigDecimal totalRangesSize = new BigDecimal(rangesSize);
    BigDecimal remainingTotalSize = totalRangesSize.multiply(new BigDecimal(fraction));
    for (ByteKeyRange range : ranges) {
      BigDecimal rangeSize = new BigDecimal(range.getSize());
      if (rangeSize.compareTo(remainingTotalSize) <= 0){
        remainingTotalSize = remainingTotalSize.subtract(rangeSize);
      } else {
        BigDecimal rangeFraction = remainingTotalSize.divide(rangeSize, 3, RoundingMode.FLOOR);
        return range.interpolateKey(rangeFraction.doubleValue());
      }
    }
    return null;
  }

  /**
   * Calculates the combined estimated size of all ranges in this tracker. Must be called when
   * {@code ranges} is changed.
   */
  private synchronized void calculateRangesSize(){
    rangesSize = BigInteger.ZERO;
    for (ByteKeyRange range : ranges) {
      rangesSize = rangesSize.add(range.getSize());
    }
  }

  ///////////////////////////////////////////////////////////////////////////////
  private List<ByteKeyRange> ranges;
  private BigInteger rangesSize;

  private ByteKeyRangesTracker(List<ByteKeyRange> ranges) {
    this.ranges = new ArrayList<>(ranges);
    position = null;

    Collections.sort(this.ranges, new Comparator<ByteKeyRange>() {

      @Override public int compare(ByteKeyRange range1, ByteKeyRange range2) {
        return range1.getStartKey().compareTo(range2.getStartKey());
      }
    });
    calculateRangesSize();
  }

  @Override public synchronized String toString() {
    ToStringHelper helper = toStringHelper(ByteKeyRangesTracker.class);
    for (int i = 0; i < ranges.size(); i++) {
      helper.add("range" + i, ranges.get(i));
    }
    return helper.add("position", position).toString();
  }
}
