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
 * Implementation of {@link WritableBuffer} for heap-based, non-native byte order.
 *
 * @author Roman Leventov
 * @author Lee Rhodes
 */
final class LeafHeapNonNativeWritableBuffer extends NonNativeWritableBuffer {
  private static final int id = BUFFER | NONNATIVE | HEAP;
  private final Object unsafeObj;
  private final MemoryRequestServer memReqSvr;
  private final byte typeId;

  LeafHeapNonNativeWritableBuffer(
      final Object unsafeObj,
      final long regionOffset,
      final long capacityBytes,
      final int typeId,
      final MemoryRequestServer memReqSvr) {
    super(unsafeObj, 0L, regionOffset, capacityBytes);
    this.unsafeObj = unsafeObj;
    this.memReqSvr = memReqSvr;
    this.typeId = (byte) (id | (typeId & 0x7));
  }

  @Override
  BaseWritableBuffer toWritableRegion(final long offsetBytes, final long capacityBytes,
      final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | REGION;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafWritableBuffer(
            unsafeObj, getRegionOffset(offsetBytes), capacityBytes, type, memReqSvr)
        : new LeafHeapNonNativeWritableBuffer(
            unsafeObj, getRegionOffset(offsetBytes), capacityBytes, type, memReqSvr);
  }

  @Override
  BaseWritableBuffer toDuplicate(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | DUPLICATE;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafWritableBuffer(
            unsafeObj, getRegionOffset(0), getCapacity(), type, memReqSvr)
        : new LeafHeapNonNativeWritableBuffer(
            unsafeObj, getRegionOffset(0), getCapacity(), type, memReqSvr);
  }

  @Override
  BaseWritableMemory toWritableMemory(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly);
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafWritableMemory(
            unsafeObj, getRegionOffset(0), getCapacity(), type, memReqSvr)
        : new LeafHeapNonNativeWritableMemory(
            unsafeObj, getRegionOffset(0), getCapacity(), type, memReqSvr);
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return memReqSvr;
  }

  @Override
  int getTypeId() {
    return typeId & 0xff;
  }

  @Override
  Object getUnsafeObject() {
    return unsafeObj;
  }

}
