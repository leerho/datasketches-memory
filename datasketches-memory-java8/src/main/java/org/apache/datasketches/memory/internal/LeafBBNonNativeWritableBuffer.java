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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableBuffer;

/**
 * Implementation of {@link WritableBuffer} for ByteBuffer, non-native byte order.
 *
 * @author Roman Leventov
 * @author Lee Rhodes
 */
final class LeafBBNonNativeWritableBuffer extends NonNativeWritableBuffer {
  private static final int id = BUFFER | NONNATIVE | BYTEBUF;
  private final Object unsafeObj;
  private final long nativeBaseOffset; //used to compute cumBaseOffset
  private final ByteBuffer byteBuf; //holds a reference to a ByteBuffer until we are done with it.
  private final MemoryRequestServer memReqSvr;
  private final byte typeId;

  LeafBBNonNativeWritableBuffer(
      final Object unsafeObj,
      final long nativeBaseOffset,
      final long regionOffset,
      final long capacityBytes,
      final int typeId,
      final ByteBuffer byteBuf,
      final MemoryRequestServer memReqSvr) {
    super(unsafeObj, nativeBaseOffset, regionOffset, capacityBytes);
    this.unsafeObj = unsafeObj;
    this.nativeBaseOffset = nativeBaseOffset;
    this.byteBuf = byteBuf;
    this.memReqSvr = memReqSvr;
    this.typeId = (byte) (id | (typeId & 0x7));
  }

  @Override
  BaseWritableBuffer toWritableRegion(final long offsetBytes, final long capacityBytes,
      final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | REGION;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafBBWritableBuffer(
          unsafeObj, nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, byteBuf, memReqSvr)
        : new LeafBBNonNativeWritableBuffer(
          unsafeObj, nativeBaseOffset, getRegionOffset(offsetBytes), capacityBytes, type, byteBuf, memReqSvr);
  }

  @Override
  BaseWritableBuffer toDuplicate(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly) | DUPLICATE;
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafBBWritableBuffer(
            unsafeObj, nativeBaseOffset, getRegionOffset(0), getCapacity(), type, byteBuf, memReqSvr)
        : new LeafBBNonNativeWritableBuffer(
            unsafeObj, nativeBaseOffset, getRegionOffset(0), getCapacity(), type, byteBuf, memReqSvr);
  }

  @Override
  BaseWritableMemory toWritableMemory(final boolean readOnly, final ByteOrder byteOrder) {
    final int type = setReadOnlyType(typeId, readOnly);
    return Util.isNativeByteOrder(byteOrder)
        ? new LeafBBWritableMemory(
            unsafeObj, nativeBaseOffset, getRegionOffset(0), getCapacity(), type, byteBuf, memReqSvr)
        : new LeafBBNonNativeWritableMemory(
            unsafeObj, nativeBaseOffset, getRegionOffset(0), getCapacity(), type, byteBuf, memReqSvr);
  }

  @Override
  public ByteBuffer getByteBuffer() {
    checkAlive();
    return byteBuf;
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
  Object getUnsafeObject() {
    checkAlive();
    return unsafeObj;
  }

}
