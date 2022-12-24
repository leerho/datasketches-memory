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
import org.apache.datasketches.memory.WritableMemory;

/**
 * Implementation of {@link WritableMemory} for map memory, non-native byte order.
 *
 * @author Roman Leventov
 * @author Lee Rhodes
 */
final class LeafMapNonNativeWritableMemory extends NonNativeWritableMemory {
  private static final int id = MEMORY | NONNATIVE | MAP;
  private final long nativeBaseOffset; //used to compute cumBaseOffset
  private final StepBoolean valid; //a reference only
  private final AllocateDirectWritableMap dirWMap;
  private final byte typeId;

  LeafMapNonNativeWritableMemory(
      final long nativeBaseOffset,
      final long regionOffset,
      final long capacityBytes,
      final int typeId,
      final AllocateDirectWritableMap dirWMap) {
    super(null, nativeBaseOffset, regionOffset, capacityBytes);
    this.nativeBaseOffset = nativeBaseOffset;
    this.dirWMap = dirWMap;
    this.valid = dirWMap.getValid();
    this.typeId = (byte) (id | (typeId & 0x7));
  }

  @Override
  BaseWritableMemory toWritableRegion(final long offsetBytes, final long capacityBytes,
      final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | REGION;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafMapWritableMemory(
            nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, dirWMap)
        : new LeafMapNonNativeWritableMemory(
            nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, dirWMap);
  }

  @Override
  BaseWritableBuffer toWritableBuffer(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly);
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafMapWritableBuffer(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, dirWMap)
        : new LeafMapNonNativeWritableBuffer(
            nativeBaseOffset, getRegionOffset(0), getCapacity(), type, dirWMap);
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return null;
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
