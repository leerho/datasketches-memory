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

import static jdk.incubator.foreign.MemoryAccess.getByteAtOffset;
import static org.apache.datasketches.memory.internal.Util.characterPad;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.Resource;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;

/**
 * Keeps key configuration state for MemoryImpl and BufferImpl plus some common static variables
 * and check methods.
 *
 * @author Lee Rhodes
 */
abstract class ResourceImpl implements Resource {
  static final String JDK; //must be at least "1.8"
  static final int JDK_MAJOR; //8, 11, 17, etc

  //Used to convert "type" to bytes:  bytes = longs << LONG_SHIFT
  static final int BOOLEAN_SHIFT    = 0;
  static final int BYTE_SHIFT       = 0;
  static final long SHORT_SHIFT     = 1;
  static final long CHAR_SHIFT      = 1;
  static final long INT_SHIFT       = 2;
  static final long LONG_SHIFT      = 3;
  static final long FLOAT_SHIFT     = 2;
  static final long DOUBLE_SHIFT    = 3;

  //class type IDs. Do not change the bit orders
  //The first 3 bits are set dynamically
  // 0000 0XXX Group 1
  static final int READONLY  = 1;
  static final int REGION    = 1 << 1;
  static final int DUPLICATE = 1 << 2; //for Buffer only

  // 000X X000 Group 2
  static final int HEAP   = 0;
  static final int DIRECT = 1 << 3;
  static final int MAP    = 1 << 4; //Map is always Direct also

  // 00X0 0000 Group 3
  static final int NATIVE    = 0;
  static final int NONNATIVE = 1 << 5;

  // 0X00 0000 Group 4
  static final int MEMORY = 0;
  static final int BUFFER = 1 << 6;

  // X000 0000 Group 5
  static final int BYTEBUF = 1 << 7;

  /**
   * The java line separator character as a String.
   */
  static final String LS = System.getProperty("line.separator");

  static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

  static final ByteOrder NON_NATIVE_BYTE_ORDER =
      (NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

  static {
    final String jdkVer = System.getProperty("java.version");
    final int[] p = parseJavaVersion(jdkVer);
    JDK = p[0] + "." + p[1];
    JDK_MAJOR = (p[0] == 1) ? p[1] : p[0];
  }

  final MemorySegment seg;
  final int typeId;

  MemoryRequestServer memReqSvr; //selected by the user

  /**
   * Constructor
   * @param seg the MemorySegment
   * @param typeId identifies the type parameters for this Memory
   * @param memReqSvr the MemoryRequestServer
   */
  ResourceImpl(final MemorySegment seg, final int typeId, final MemoryRequestServer memReqSvr) {
    this.seg = seg;
    this.typeId = typeId;
    this.memReqSvr = memReqSvr;
  }

  //**STATIC METHODS*****************************************

  /**
   * Check the requested offset and length against the allocated size.
   * The invariants equation is: {@code 0 <= reqOff <= reqLen <= reqOff + reqLen <= allocSize}.
   * If this equation is violated an {@link IllegalArgumentException} will be thrown.
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size.
   * @throws IllegalArgumentException for exceeding address bounds
   */
  static void checkBounds(final long reqOff, final long reqLen, final long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new IllegalArgumentException(
          "reqOffset: " + reqOff + ", reqLength: " + reqLen
              + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  static void checkJavaVersion(final String jdkVer, final int p0) {
    boolean ok = p0 == 17;
    if (!ok) { throw new IllegalArgumentException(
        "Unsupported JDK Major Version, must be 17; " + jdkVer);
    }
  }

  private static String pad(final String s, final int fieldLen) {
    return characterPad(s, fieldLen, ' ' , true);
  }

  /**
   * Returns first two number groups of the java version string.
   * @param jdkVer the java version string from System.getProperty("java.version").
   * @return first two number groups of the java version string.
   * @throws IllegalArgumentException for an improper Java version string.
   */
  static int[] parseJavaVersion(final String jdkVer) {
    final int p0, p1;
    try {
      String[] parts = jdkVer.trim().split("^0-9\\.");//grab only number groups and "."
      parts = parts[0].split("\\."); //split out the number groups
      p0 = Integer.parseInt(parts[0]); //the first number group
      p1 = (parts.length > 1) ? Integer.parseInt(parts[1]) : 0; //2nd number group, or 0
    } catch (final NumberFormatException | ArrayIndexOutOfBoundsException  e) {
      throw new IllegalArgumentException("Improper Java -version string: " + jdkVer + "\n" + e);
    }
    checkJavaVersion(jdkVer, p0);
    return new int[] {p0, p1};
  }

  static final WritableBuffer selectBuffer( //Java 17 only
      final MemorySegment segment,
      final int type,
      final MemoryRequestServer memReqSvr,
      final boolean byteBufferType,
      final boolean mapType,
      final boolean nativeBOType) {
    final MemoryRequestServer memReqSvr2 = (byteBufferType || mapType) ? null : memReqSvr;
    final WritableBuffer wbuf;
    if (nativeBOType) {
      wbuf = new NativeWritableBufferImpl(segment, type, memReqSvr2);
    } else { //non-native BO
      wbuf = new NonNativeWritableBufferImpl(segment, type, memReqSvr2);
    }
    return wbuf;
  }

  static final WritableMemory selectMemory( //Java 17 only
      final MemorySegment segment,
      final int type,
      final MemoryRequestServer memReqSvr,
      final boolean byteBufferType,
      final boolean mapType,
      final boolean nativeBOType) {
    final MemoryRequestServer memReqSvr2 = (byteBufferType || mapType) ? null : memReqSvr;
    final WritableMemory wmem;
    if (nativeBOType) {
      wmem = new NativeWritableMemoryImpl(segment, type, memReqSvr2);
    } else { //non-native BO
      wmem = new NonNativeWritableMemoryImpl(segment, type, memReqSvr2);
    }
    return wmem;
  }

  /**
   * Returns a formatted hex string of an area of this object.
   * Used primarily for testing.
   * @param resource the ResourceImpl
   * @param comment optional unique description
   * @param offsetBytes offset bytes relative to the MemoryImpl start
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  static final String toHex(final ResourceImpl resource, final String comment, final long offsetBytes,
      final int lengthBytes, final boolean withData) {
    final MemorySegment seg = resource.seg;
    final long capacity = seg.byteSize();
    checkBounds(offsetBytes, lengthBytes, capacity);

    final String theComment = (comment != null) ? comment : "";
    final String addHCStr = "" + Integer.toHexString(seg.address().hashCode());
    final MemoryRequestServer memReqSvr = resource.getMemoryRequestServer();
    final String memReqStr = memReqSvr != null
        ? memReqSvr.getClass().getSimpleName() + ", " + Integer.toHexString(memReqSvr.hashCode())
        : "null";

    final StringBuilder sb = new StringBuilder();
    sb.append(LS + "### DataSketches Memory Component SUMMARY ###").append(LS);
    sb.append("Header Comment       : ").append(theComment).append(LS);
    sb.append("TypeId String          : ").append(typeDecode(resource.typeId)).append(LS);
    sb.append("OffsetBytes            : ").append(offsetBytes).append(LS);
    sb.append("LengthBytes            : ").append(lengthBytes).append(LS);
    sb.append("Capacity               : ").append(capacity).append(LS);
    sb.append("MemoryAddress hashCode : ").append(addHCStr).append(LS);
    sb.append("MemReqSvr, hashCode    : ").append(memReqStr).append(LS);
    sb.append("Read Only              : ").append(resource.isReadOnly()).append(LS);
    sb.append("Type Byte Order        : ").append(resource.getByteOrder().toString()).append(LS);
    sb.append("Native Byte Order      : ").append(NATIVE_BYTE_ORDER.toString()).append(LS);
    sb.append("JDK Runtime Version    : ").append(JDK).append(LS);
    //Data detail
    if (withData) {
      sb.append("Data, LittleEndian     :  0  1  2  3  4  5  6  7");
      for (long i = 0; i < lengthBytes; i++) {
        final int b = getByteAtOffset(seg, offsetBytes + i) & 0XFF;
        if (i % 8 == 0) { //row header
          sb.append(String.format("%n%23s: ", offsetBytes + i));
        }
        sb.append(String.format("%02x ", b));
      }
    }
    sb.append(LS + "### END SUMMARY ###");
    sb.append(LS);

    return sb.toString();
  }

  /**
   * Decodes the resource type. This is primarily for debugging.
   * @param typeId the given typeId
   * @return a human readable string.
   */
  static final String typeDecode(final int typeId) {
    final StringBuilder sb = new StringBuilder();
    final int group1 = typeId & 0x7;
    switch (group1) { // 0000 0XXX
      case 0 : sb.append(pad("Writable + ",32)); break;
      case 1 : sb.append(pad("ReadOnly + ",32)); break;
      case 2 : sb.append(pad("Writable + Region + ",32)); break;
      case 3 : sb.append(pad("ReadOnly + Region + ",32)); break;
      case 4 : sb.append(pad("Writable + Duplicate + ",32)); break;
      case 5 : sb.append(pad("ReadOnly + Duplicate + ",32)); break;
      case 6 : sb.append(pad("Writable + Region + Duplicate + ",32)); break;
      case 7 : sb.append(pad("ReadOnly + Region + Duplicate + ",32)); break;
      default: break;
    }
    final int group2 = (typeId >>> 3) & 0x3;
    switch (group2) { // 000X X000
      case 0 : sb.append(pad("Heap + ",15)); break;
      case 1 : sb.append(pad("Direct + ",15)); break;
      case 2 : sb.append(pad("Map + Direct + ",15)); break;
      case 3 : sb.append(pad("Map + Direct + ",15)); break;
      default: break;
    }
    final int group3 = (typeId >>> 5) & 0x1;
    switch (group3) { // 00X0 0000
      case 0 : sb.append(pad("NativeOrder + ",17)); break;
      case 1 : sb.append(pad("NonNativeOrder + ",17)); break;
      default: break;
    }
    final int group4 = (typeId >>> 6) & 0x1;
    switch (group4) { // 0X00 0000
      case 0 : sb.append(pad("Memory + ",9)); break;
      case 1 : sb.append(pad("Buffer + ",9)); break;
      default: break;
    }
    final int group5 = (typeId >>> 7) & 0x1;
    switch (group5) { // X000 0000
      case 0 : sb.append(pad("",10)); break;
      case 1 : sb.append(pad("ByteBuffer",10)); break;
      default: break;
    }
    return sb.toString();
  }

  //**NON STATIC METHODS*****************************************

  @Override //Java 17 only
  public final ByteBuffer asByteBufferView(final ByteOrder order) {
    final ByteBuffer byteBuf = seg.asByteBuffer().order(order);
    return byteBuf;
  }

  //@SuppressWarnings("resource")
  @Override //Java 17 only
  public void close() { //moved here
    if (seg != null && seg.scope().isAlive() && !seg.scope().isImplicit()) {
      if (seg.isNative() || seg.isMapped()) {
        seg.scope().close();
      }
    }
  }

  @Override
  public final boolean equalTo(final long thisOffsetBytes, final Resource that,
      final long thatOffsetBytes, final long lengthBytes) {
    if (that == null) { return false; }
    return CompareAndCopy.equals(seg, thisOffsetBytes, ((ResourceImpl) that).seg, thatOffsetBytes, lengthBytes);
  }

  @Override //Java 17 only
  public void force() { seg.force(); } //moved here

  @Override
  public final ByteOrder getByteOrder() {
    return (typeId & NONNATIVE) > 0 ? NON_NATIVE_BYTE_ORDER : NATIVE_BYTE_ORDER;
  }

  @Override
  public final long getCapacity() {
    return seg.byteSize();
  }

  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return memReqSvr;
  }

  //@SuppressWarnings("resource")
  @Override
  public boolean isAlive() {
    return seg.scope().isAlive();
  }

  @Override
  public final boolean isByteBufferResource() {
    return (typeId & BYTEBUF) > 0;
  }

  @Override
  public final boolean isByteOrderCompatible(final ByteOrder byteOrder) {
    final ByteOrder typeBO = getByteOrder();
    return typeBO == NATIVE_BYTE_ORDER && typeBO == byteOrder;
  }

  @Override
  public final boolean isDirectResource() {
    assert seg.isNative() == (typeId & DIRECT) > 0;
    return seg.isNative();
  }

  @Override
  public final boolean isDuplicateBufferView() {
    return (typeId & DUPLICATE) > 0;
  }

  @Override //Java 17 only
  public boolean isLoaded() { return seg.isLoaded(); }

  @Override
  public boolean isMemoryMappedFileResource() {
    assert seg.isMapped() == (typeId & MAP) > 0;
    return seg.isMapped();
  }

  @Override
  public final boolean isMemoryApi() {
    return (typeId & BUFFER) == 0;
  }

  @Override
  public boolean isNonNativeOrder() {
    return (typeId & NONNATIVE) > 0;
  }

  @Override
  public final boolean isReadOnly() {
    assert seg.isReadOnly() == (typeId & READONLY) > 0;
    return seg.isReadOnly();
  }

  @Override
  public final boolean isRegionView() { //isRegionView
    return (typeId & REGION) > 0;
  }

  @Override //Java 17 only
  public void load() { seg.load(); } //moved here

  @Override
  public long mismatch(final Resource that) { //Java 17 only
    Objects.requireNonNull(that);
    if (!that.isAlive()) { throw new IllegalArgumentException("Given argument is not alive."); }
    ResourceImpl thatBSI = (ResourceImpl) that;
    return seg.mismatch(thatBSI.seg);
  }

  @Override //Java 17 only
  public final long nativeOverlap(final Resource that) { //Java 17 only
    if (that == null) { return 0; }
    if (!that.isAlive()) { return 0; }
    ResourceImpl thatBSI = (ResourceImpl) that;
    if (this == thatBSI) { return seg.byteSize(); }
    return nativeOverlap(seg, thatBSI.seg);
  }

  static final long nativeOverlap(final MemorySegment segA, final MemorySegment segB) { // //Java 17 only; used in test
    if (!segA.isNative() || !segB.isNative()) { return 0; } //both segments must be native
    //Assume that memory addresses increase from left to right.
    //Identify the left and right edges of two regions, A and B in memory.
    final long bytesA = segA.byteSize();
    final long bytesB = segB.byteSize();
    final long lA = segA.address().toRawLongValue(); //left A
    final long lB = segB.address().toRawLongValue(); //left B
    final long rA = lA + bytesA; //right A
    final long rB = lB + bytesB; //right B
    if ((rA <= lB) || (rB <= lA)) { return 0; } //Eliminate the totally disjoint case:

    final long result = (bytesA == bytesB) //Two major cases: equal and not equal in size
        ? nativeOverlapEqualSizes(lA, rA, lB, rB)
        : nativeOverlapNotEqualSizes(lA, rA, lB, rB);

    return (lB < lA) ? -result : result; //if lB is lower in memory than lA, we return a negative result
  }

  private static final long nativeOverlapEqualSizes(final long lA, final long rA, final long lB, final long rB) {
    if (lA == lB) { return rA - lA; } //Exact overlap, return either size
    return (lA < lB)
        ? rA - lB  //Partial overlap on right portion of A
        : rB - lA; //else partial overlap on left portion of A
  }

  private static final long nativeOverlapNotEqualSizes(final long lA, final long rA, final long lB, final long rB) {
    return (rB - lB < rA - lA) //whichever is larger we assign to parameters 1 and 2
        ? biggerSmaller(lA, rA, lB, rB)  //A bigger than B
        : biggerSmaller(lB, rB, lA, rA); //B bigger than A, reverse parameters
  }

  private static final long biggerSmaller(long lLarge, long rLarge, long lSmall, long rSmall) {
    if ((rSmall <= rLarge) && (lLarge <= lSmall)) { return rSmall - lSmall; } //Small is totally within Large
    return (rLarge < rSmall)
        ? rLarge - lSmall  //Partial overlap on right portion of Large
        : rSmall - lLarge; //Partial overlap on left portion of Large
  }

  @Override
  public ResourceScope scope() { return seg.scope(); } //Java 17 only

  @Override
  public ByteBuffer toByteBuffer(final ByteOrder order) { //Java 17 only
    Objects.requireNonNull(order, "The input ByteOrder must not be null");
    return ByteBuffer.wrap(seg.toByteArray());
  }

  @Override
  public final String toHexString(final String comment, final long offsetBytes, final int lengthBytes,
      final boolean withData) {
    return toHex(this, comment, offsetBytes, lengthBytes, withData);
  }

  @Override //Java 17 only
  public MemorySegment toMemorySegment() {
    final MemorySegment arrSeg = MemorySegment.ofArray(new byte[(int)seg.byteSize()]);
    arrSeg.copyFrom(seg);
    return arrSeg;
  }

  @Override //Java 17 only
  public void unload() { seg.unload(); } //moved here

  @Override
  public final long xxHash64(final long in, final long seed) {
    return XxHash64.hash(in, seed);
  }

  @Override
  public final long xxHash64(final long offsetBytes, final long lengthBytes, final long seed) {
    return XxHash64.hash(seg, offsetBytes, lengthBytes, seed);
  }

}
