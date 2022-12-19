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

package org.apache.datasketches.memory;

import java.nio.ByteOrder;

/**
 * Keeps key configuration state for Memory and Buffer plus some common static variables
 * and check methods.
 *
 * @author Lee Rhodes
 */
public interface BaseState {

  /**
   * Currently used only for test, hold for possible future use
   */
  static final MemoryRequestServer defaultMemReqSvr = null; //new DefaultMemoryRequestServer();

  /**
   * Returns true if the given object is an instance of this class and has equal contents to
   * this object.
   * @param that the given BaseState object
   * @return true if the given object has equal contents to this object.
   */
  default boolean equalTo(BaseState that) {
    if (that == null || this.getCapacity() != that.getCapacity()) return false;
    return equalTo(0, that, 0, that.getCapacity());
  }

  /**
   * Returns true if the given object is an instance of this class and has equal contents to
   * this object in the given range of bytes. This will also check two distinct ranges within the
   * same object for equals.
   * @param thisOffsetBytes the starting offset in bytes for this object.
   * @param that the given BaseState object
   * @param thatOffsetBytes the starting offset in bytes for the given BaseState object
   * @param lengthBytes the size of the range in bytes
   * @return true if the given BaseState object has equal contents to this object in the given range of bytes.
   */
  boolean equalTo(long thisOffsetBytes, BaseState that, long thatOffsetBytes, long lengthBytes);

  /**
   * Gets the current Type ByteOrder.
   * This may be different from the ByteOrder of the backing resource and of the Native Byte Order.
   * @return the current Type ByteOrder.
   */
  ByteOrder getByteOrder();

  /**
   * Gets the capacity of this object in bytes
   * @return the capacity of this object in bytes
   */
  long getCapacity();

  /**
   * Returns the MemoryRequestSever or null, if it has not been configured.
   * @return the MemoryRequestSever or null, if it has not been configured.
   */
  MemoryRequestServer getMemoryRequestServer();

  /**
   * Is the underlying resource alive?
   * @return true, if the underlying resource is alive.
   * @See {@link java.lang.AutoCloseable#close()}
   */
  boolean isAlive();

  /**
   * Returns true if this Memory is backed by a ByteBuffer.
   * @return true if this Memory is backed by a ByteBuffer.
   */
  boolean isByteBufferResource();

  /**
   * Returns true if the Native ByteOrder is the same as the ByteOrder of the
   * current Buffer or Memory and the same ByteOrder as the given byteOrder.
   * @param byteOrder the given ByteOrder
   * @return true if the Native ByteOrder is the same as the ByteOrder of the
   * current Buffer or Memory and the same ByteOrder as the given byteOrder.
   */
  boolean isByteOrderCompatible(ByteOrder byteOrder);

  /**
   * If true, the backing resource is direct (off-heap) memory.
   * This is the case for allocated direct memory, memory mapped files,
   * or from a wrapped ByteBuffer that was allocated direct.
   * If false, the backing resource is the normal Java heap.
   * @return true if the backing resource is direct (off-heap) memory.
   */
  boolean isDirectResource();

  /**
   * Returns true if this instance is a duplicate of a Buffer instance.
   * @return true if this instance is a duplicate of a Buffer instance.
   */
  boolean isDuplicateBufferView();

  /**
   * If true, this is a <i>Memory</i> or <i>WritableMemory</i> instance, which provides
   * the Memory API.
   * The Memory API is the principal API for this Memory Component.
   * It provides a rich variety of direct manipulations of four types of resources:
   * On-heap memory, direct (off-heap) memory, memory-mapped files, and ByteBuffers.
   * If false, this is a <i>Buffer</i> or <i>WritableBuffer</i> instance, which provides the Buffer API.
   * The Buffer API is largely parallel to the Memory API except that it adds a positional API
   * similar to that in <i>ByteBuffer</i>.  The positional API is a convenience when iterating over structured
   * arrays, or buffering input or output streams (thus the name).
   * @return true if this is a Buffer or WritableBuffer instance, which provides the Buffer API.
   */
  boolean isMemoryApi();

  /**
   * Returns true if the backing resource is a memory mapped file.
   * @return true if the backing resource is a memory mapped file.
   */
  boolean isMemoryMappedFileResource();

  /**
   * If true, all put and get operations will assume the non-native ByteOrder.
   * Otherwise, all put and get operations will assume the native ByteOrder.
   * @return true, if all put and get operations will assume the non-native ByteOrder.
   */
  boolean isNonNativeOrder();

  /**
   * Returns true if this or the backing resource is read-only.
   * @return true if this or the backing resource is read-only.
   */
  boolean isReadOnly();

  /**
   * Returns true if this instance is a region view of another Memory or Buffer
   * @return true if this instance is a region view of another Memory or Buffer
   */
  boolean isRegionView();

  /**
   * Returns a description of this object with an optional formatted hex string of the data
   * for the specified a range. Used primarily for testing.
   * @param comment a description
   * @param offsetBytes offset bytes relative to this object start
   * @param lengthBytes number of bytes to convert to a hex string
   * @param withData include output listing of byte data in the given range
   * @return a description and hex output in a human readable format.
   */
  String toHexString(String comment, long offsetBytes, int lengthBytes, boolean withData);

  /**
   * Returns a 64-bit hash from a single long. This method has been optimized for speed when only
   * a single hash of a long is required.
   * @param in A long.
   * @param seed A long valued seed.
   * @return the hash.
   */
  long xxHash64(long in, long seed);

  /**
   * Returns the 64-bit hash of the sequence of bytes in this object specified by
   * <i>offsetBytes</i>, <i>lengthBytes</i> and a <i>seed</i>.  Note that the sequence of bytes is
   * always processed in the same order independent of endianness.
   *
   * @param offsetBytes the given offset in bytes to the first byte of the byte sequence.
   * @param lengthBytes the given length in bytes of the byte sequence.
   * @param seed the given long seed.
   * @return the 64-bit hash of the sequence of bytes in this object specified by
   * <i>offsetBytes</i> and <i>lengthBytes</i>.
   */
  long xxHash64(long offsetBytes, long lengthBytes, long seed);

  //DEPRECATED. NOT SUPPORTED AS OF JAVA 17+ VERSIONS

  /**
   * Returns true if the backing resource of <i>this</i> is identical with the backing resource
   * of <i>that</i>. The capacities must be the same.  If <i>this</i> is a region,
   * the region offset must also be the same.
   * @param that A different non-null object
   * @return true if the backing resource of <i>this</i> is the same as the backing resource
   * of <i>that</i>.
   * @deprecated no longer supported as of Java 17 versions.
   * With Java 17 use nativeOverlap(other) instead.
   */
  @Deprecated
  boolean isSameResource(Object that);

}
