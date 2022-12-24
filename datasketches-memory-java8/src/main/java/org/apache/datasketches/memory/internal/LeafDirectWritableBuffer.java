/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.memory.internal;

import java.nio.ByteOrder;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableBuffer;

/**
 * Implementation of {@link WritableBuffer} for direct memory, native byte order.
 *
 * @author Roman Leventov
 * @author Lee Rhodes
 */
final class LeafDirectWritableBuffer extends NativeWritableBuffer {
  private static final int id = BUFFER | NATIVE | DIRECT;
  private final long nativeBaseOffset; //used to compute cumBaseOffset
  private final StepBoolean valid; //a reference only
  private final MemoryRequestServer memReqSvr;
  private final byte typeId;

  LeafDirectWritableBuffer(
      final long nativeBaseOffset,
      final long regionOffset,
      final long capacityBytes,
      final int typeId,
      final StepBoolean valid,
      final MemoryRequestServer memReqSvr) {
    super(null, nativeBaseOffset, regionOffset, capacityBytes);
    this.nativeBaseOffset = nativeBaseOffset;
    this.valid = valid;
    this.memReqSvr = memReqSvr;
    this.typeId = (byte) (id | (typeId & 0x7));
  }

  @Override
  BaseWritableBuffer toWritableRegion(final long offsetBytes, final long capacityBytes,
      final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | REGION;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafDirectWritableBuffer(
            nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, valid, memReqSvr)
        : new LeafDirectNonNativeWritableBuffer(
            nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, valid, memReqSvr);
  }

  @Override
  BaseWritableBuffer toDuplicate(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | DUPLICATE;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafDirectWritableBuffer(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, valid, memReqSvr)
        : new LeafDirectNonNativeWritableBuffer(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, valid, memReqSvr);
  }

  @Override
  BaseWritableMemory toWritableMemory(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly);
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafDirectWritableMemory(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, valid, memReqSvr)
        : new LeafDirectNonNativeWritableMemory(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, valid, memReqSvr);
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    checkAlive();
    return memReqSvr;
  }

  @Override
  long getNativeBaseOffset() {
    return nativeBaseOffset;
  }

  @Override
  int getTypeId() {
    return typeId & 0xff;
  }

  @Override
  public boolean isAlive() {
    return valid.get();
  }

}
