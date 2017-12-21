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

/**
 * A {@link RangeTracker} for {@link ByteKey ByteKeys} in {@link ByteKeyRange ByteKeyRanges}.
 *
 * @see ByteKey
 * @see ByteKeyRange
 */
public final class ByteKeyRangeTracker extends AbstractByteKeyRangeTracker {

  /** Instantiates a new {@link ByteKeyRangeTracker} with the specified range. */
  public static ByteKeyRangeTracker of(ByteKeyRange range) {
    return new ByteKeyRangeTracker(range);
  }

  @Override
  public synchronized ByteKey getStartPosition() {
    return range.getStartKey();
  }

  @Override
  public synchronized ByteKey getStopPosition() {
    return range.getEndKey();
  }

  @Override
  public void setEndPosition(ByteKey end) {
    range = range.withEndKey(end);
  }

  @Override
  public void setStartPosition(ByteKey start) {
    range = range.withStartKey(start);
  }

  @Override public
  boolean containsKey(ByteKey key) {
    return range.containsKey(key);
  }

  /** Returns the current range. */
  public synchronized ByteKeyRange getRange() {
    return range;
  }

  @Override
  public synchronized double getFractionConsumed() {
    if (position == null) {
      return 0;
    } else if (done) {
      return 1.0;
    } else if (position.compareTo(range.getEndKey()) >= 0) {
      return 1.0;
    }

    return range.estimateFractionForKey(position);
  }

  ///////////////////////////////////////////////////////////////////////////////
  private ByteKeyRange range;

  private ByteKeyRangeTracker(ByteKeyRange range) {
    this.range = range;
  }

  @Override
  public synchronized String toString() {
    return toStringHelper(ByteKeyRangeTracker.class)
        .add("range", range)
        .add("position", position)
        .toString();
  }
}
