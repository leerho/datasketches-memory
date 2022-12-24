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

import static org.apache.datasketches.memory.internal.UnsafeUtil.unsafe;
import static org.apache.datasketches.memory.internal.Util.characterPad;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.memory.BoundsException;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.ReadOnlyException;
import org.apache.datasketches.memory.Resource;

/**
 * Implements the root Resource methods.
 *
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public abstract class ResourceImpl implements Resource {
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

  public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();

  static final ByteOrder NON_NATIVE_BYTE_ORDER =
      (NATIVE_BYTE_ORDER == ByteOrder.LITTLE_ENDIAN) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;

  static {
    final String jdkVer = System.getProperty("java.version");
    final int[] p = parseJavaVersion(jdkVer);
    JDK = p[0] + "." + p[1];
    JDK_MAJOR = (p[0] == 1) ? p[1] : p[0];
  }

  final long capacityBytes_; //NOT USED in JDK 17

  /**
   * This becomes the base offset used by all Unsafe calls. It is cumulative in that in includes
   * all offsets from regions, user-defined offsets when creating MemoryImpl, and the array object
   * header offset when creating MemoryImpl from primitive arrays.
   */
  final long cumBaseOffset_; //NOT USED in JDK 17

  MemoryRequestServer memReqSvr = null; //selected by the user

  /**
   * Constructor
   * @param unsafeObj The primitive backing array. It may be null. Used by Unsafe calls.
   * @param nativeBaseOffset The off-heap memory address including DirectByteBuffer split offsets.
   * @param regionOffset This offset defines address zero of this object (usually a region)
   * relative to address zero of the backing resource. It is used to compute cumBaseOffset.
   * This will be loaded from heap ByteBuffers, which have a similar field used for slices.
   * It is used by region() and writableRegion().
   * This offset does not include the size of an object array header, if there is one.
   * @param capacityBytes the capacity of this object. Used by all methods when checking bounds.
   */
  ResourceImpl(final Object unsafeObj, final long nativeBaseOffset, final long regionOffset,
      final long capacityBytes) {
    capacityBytes_ = capacityBytes;
    cumBaseOffset_ = regionOffset + (unsafeObj == null
        ? nativeBaseOffset
        : UnsafeUtil.getArrayBaseOffset(unsafeObj.getClass()));
  }

  //**STATIC METHODS*****************************************

  /**
   * Check the requested offset and length against the allocated size.
   * The invariants equation is: {@code 0 <= reqOff <= reqLen <= reqOff + reqLen <= allocSize}.
   * If this equation is violated an {@link IllegalArgumentException} will be thrown.
   * @param reqOff the requested offset
   * @param reqLen the requested length
   * @param allocSize the allocated size.
   */
  public static void checkBounds(final long reqOff, final long reqLen, final long allocSize) {
    if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0) {
      throw new BoundsException(
          "reqOffset: " + reqOff + ", reqLength: " + reqLen
              + ", (reqOff + reqLen): " + (reqOff + reqLen) + ", allocSize: " + allocSize);
    }
  }

  static void checkJavaVersion(final String jdkVer, final int p0, final int p1 ) {
    boolean ok = ((p0 == 1) && (p1 == 8)) || (p0 == 8) || (p0 == 11);
    if (!ok) { throw new IllegalArgumentException(
        "Unsupported JDK Major Version. It must be one of 1.8, 8, 11: " + jdkVer);
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
    checkJavaVersion(jdkVer, p0, p1);
    return new int[] {p0, p1};
  }

  //REACHABILITY FENCE
  static void reachabilityFence(final Object obj) { } //Java 8 & 11 only

  final static byte setReadOnlyType(final byte type, final boolean readOnly) {
    return (byte)((type & ~1) | (readOnly ? READONLY : 0));
  }

  /**
   * Returns a formatted hex string of an area of this object.
   * Used primarily for testing.
   * @param resource the ResourceImpl
   * @param comment a descriptive header
   * @param offsetBytes offset bytes relative to the MemoryImpl start
   * @param lengthBytes number of bytes to convert to a hex string
   * @return a formatted hex string in a human readable array
   */
  static final String toHex(final ResourceImpl resource, final String comment, final long offsetBytes,
      final int lengthBytes, final boolean withData) {
    final long capacity = resource.getCapacity();
    checkBounds(offsetBytes, lengthBytes, capacity);

    final String theComment = (comment != null) ? comment : "";
    final String s1 = String.format("(..., %d, %d)", offsetBytes, lengthBytes);
    final long hcode = resource.hashCode() & 0XFFFFFFFFL;
    final String call = ".toHexString" + s1 + ", hashCode: " + hcode;

    final Object uObj = resource.getUnsafeObject();
    final String uObjStr;
    final long uObjHeader;
    if (uObj == null) {
      uObjStr = "null";
      uObjHeader = 0;
    } else {
      uObjStr =  uObj.getClass().getSimpleName() + ", " + (uObj.hashCode() & 0XFFFFFFFFL);
      uObjHeader = UnsafeUtil.getArrayBaseOffset(uObj.getClass());
    }
    final ByteBuffer bb = resource.getByteBuffer();
    final String bbStr = bb == null ? "null"
            : bb.getClass().getSimpleName() + ", " + (bb.hashCode() & 0XFFFFFFFFL);
    final MemoryRequestServer memReqSvr = resource.getMemoryRequestServer();
    final String memReqStr = memReqSvr != null
        ? memReqSvr.getClass().getSimpleName() + ", " + (memReqSvr.hashCode() & 0XFFFFFFFFL)
        : "null";
    final long cumBaseOffset = resource.getCumulativeOffset(0);

    final StringBuilder sb = new StringBuilder();
    sb.append(LS + "### DataSketches Memory Component SUMMARY ###").append(LS);
    sb.append("Header Comment      : ").append(theComment).append(LS);
    sb.append("Call Parameters     : ").append(call);
    sb.append("UnsafeObj, hashCode : ").append(uObjStr).append(LS);
    sb.append("UnsafeObjHeader     : ").append(uObjHeader).append(LS);
    sb.append("ByteBuf, hashCode   : ").append(bbStr).append(LS);
    sb.append("RegionOffset        : ").append(resource.getRegionOffset(0)).append(LS);
    sb.append("Capacity            : ").append(capacity).append(LS);
    sb.append("CumBaseOffset       : ").append(cumBaseOffset).append(LS);
    sb.append("MemReqSvr, hashCode : ").append(memReqStr).append(LS);
    sb.append("Read Only           : ").append(resource.isReadOnly()).append(LS);
    sb.append("Type Byte Order     : ").append(resource.getByteOrder().toString()).append(LS);
    sb.append("Native Byte Order   : ").append(NATIVE_BYTE_ORDER.toString()).append(LS);
    sb.append("JDK Runtime Version : ").append(JDK).append(LS);
    //Data detail
    if (withData) {
      sb.append("Data, littleEndian  :  0  1  2  3  4  5  6  7");
      for (long i = 0; i < lengthBytes; i++) {
        final int b = unsafe.getByte(uObj, cumBaseOffset + offsetBytes + i) & 0XFF;
        if (i % 8 == 0) { //row header
          sb.append(String.format("%n%20s: ", offsetBytes + i));
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

  void checkAlive() { //Java 8 & 11 only
    if (!isAlive()) {
      throw new IllegalStateException("Memory not valid.");
    }
  }

  void checkWritable() { //Java 8 & 11 only
    if (isReadOnly()) {
      throw new ReadOnlyException("Resource is read-only.");
    }
  }

  @Override
  public final boolean equalTo(final long thisOffsetBytes, final Resource that,
      final long thatOffsetBytes, final long lengthBytes) {
    if (that == null) { return false; }
    return CompareAndCopy.equals(this, thisOffsetBytes, (ResourceImpl) that, thatOffsetBytes, lengthBytes);
  }

  @Override
  public final ByteOrder getByteOrder() {
    return isNonNativeOrder() ? NON_NATIVE_BYTE_ORDER : NATIVE_BYTE_ORDER;
  }

  //Overridden by ByteBuffer Leafs
  public ByteBuffer getByteBuffer() { //TODO Keep??
    checkAlive();
    return null;
  }

  @Override
  public final long getCapacity() {
    checkAlive();
    return capacityBytes_;
  }

  final long getCumulativeOffset(final long offsetBytes) { //Java 8 & 11 only
    checkAlive();
    return cumBaseOffset_ + offsetBytes;
  }

  //Documented in WritableMemory and WritableBuffer interfaces.
  //Overridden in the Leaf nodes; Required here by toHex(...).
  @Override
  public MemoryRequestServer getMemoryRequestServer() {
    return null;
  }

  //Overridden by ByteBuffer, Direct and Map leafs
  long getNativeBaseOffset() { //Java 8 & 11 only
    return 0;
  }

  final long getRegionOffset(final long offsetBytes) { //Java 8 & 11 only
    final Object unsafeObj = getUnsafeObject();
    return offsetBytes + (unsafeObj == null
        ? cumBaseOffset_ - getNativeBaseOffset()
        : cumBaseOffset_ - UnsafeUtil.getArrayBaseOffset(unsafeObj.getClass()));
  }

  //Overridden by all leafs
  abstract int getTypeId(); //Java 8 & 11 only

  //Overridden by Heap and ByteBuffer Leafs. Made public as getArray() in WritableMemoryImpl and
  // WritableBufferImpl
  Object getUnsafeObject() { //Java 8 & 11 only
    return null;
  }

  //Overridden by Direct and Map leafs
  @Override
  public boolean isAlive() {
    return true;
  }

  @Override
  public final boolean isByteBufferResource() { //Java 8 & 11 only
    return (getTypeId() & BYTEBUF) > 0;
  }

  @Override
  public final boolean isByteOrderCompatible(final ByteOrder byteOrder) {
    final ByteOrder typeBO = getByteOrder();
    return typeBO == NATIVE_BYTE_ORDER && typeBO == byteOrder;
  }

  @Override
  public final boolean isDirectResource() {
    final int bits = (getTypeId() >>> 3) & 3;
    return bits == 1 || bits == 3 || getUnsafeObject() == null;
  }

  @Override
  public final boolean isDuplicateBufferView() {
    return (getTypeId() & DUPLICATE) > 0;
  }

  @Override
  public final boolean isMemoryMappedFileResource() {
    return (getTypeId() >>> 3 & 3) == 2;
  }

  @Override
  public final boolean isMemoryApi() {
    return (getTypeId() & BUFFER) == 0;
  }

  @Override
  public final boolean isNonNativeOrder() {
    return (getTypeId() & NONNATIVE) > 0;
  }

  @Override
  public final boolean isReadOnly() {
    return (getTypeId() & READONLY) > 0;
  }

  @Override
  public final boolean isRegionView() {
    return (getTypeId() & REGION) > 0;
  }

  @Override //Java 8 & 11 only
  public final boolean isSameResource(final Resource that) {
    checkAlive();
    if (that == null) { return false; }
    final ResourceImpl thatR = (ResourceImpl) that;
    thatR.checkAlive();
    if (this == thatR) { return true; }

    return cumBaseOffset_ == thatR.cumBaseOffset_
            && capacityBytes_ == thatR.capacityBytes_
            && getUnsafeObject() == thatR.getUnsafeObject()
            && getByteBuffer() == thatR.getByteBuffer();
  }

  @Override
  public final String toHexString(final String comment, final long offsetBytes,
      final int lengthBytes, boolean withData) {
    checkAlive();
    return toHex(this, comment, offsetBytes, lengthBytes, withData);
  }

  @Override
  public final long xxHash64(final long offsetBytes, final long lengthBytes, final long seed) {
    checkAlive();
    return XxHash64.hash(getUnsafeObject(), cumBaseOffset_ + offsetBytes, lengthBytes, seed);
  }

  @Override
  public final long xxHash64(final long in, final long seed) {
    return XxHash64.hash(in, seed);
  }

}