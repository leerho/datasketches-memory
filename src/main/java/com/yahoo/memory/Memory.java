/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.Util.nativeOrder;
import static com.yahoo.memory.Util.negativeCheck;
import static com.yahoo.memory.Util.nullCheck;
import static com.yahoo.memory.Util.zeroCheck;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Provides read-only primitive and primitive array methods to any of the four resources
 * mentioned in the package level documentation.
 *
 * @author Roman Leventov
 * @author Lee Rhodes
 *
 * @see com.yahoo.memory
 */
public abstract class Memory extends BaseState {

  //Pass-through ctor for all parameters
  Memory(
      final Object unsafeObj, final long nativeBaseOffset, final long regionOffset,
      final long capacityBytes, final boolean readOnly, final ByteOrder dataByteOrder,
      final ByteBuffer byteBuf, final StepBoolean valid) {
    super(unsafeObj, nativeBaseOffset, regionOffset, capacityBytes, readOnly, dataByteOrder,
        byteBuf, valid);
  }

  //BYTE BUFFER XXX
  /**
   * Accesses the given ByteBuffer for read-only operations. The returned <i>Memory</i> object has the
   * same byte order, as the given ByteBuffer, unless the capacity of the given ByteBuffer is zero,
   * then endianness of the returned <i>Memory</i> object (as well as backing storage) is unspecified.
   * @param byteBuf the given ByteBuffer, must not be null
   * @return a new <i>Memory</i> for read-only operations on the given ByteBuffer.
   */
  public static Memory wrap(final ByteBuffer byteBuf) {
    return BaseWritableMemoryImpl.wrapByteBuffer(byteBuf, true, byteBuf.order());
  }

  /**
   * Accesses the given ByteBuffer for read-only operations. The returned <i>Memory</i> object has the
   * given byte order, ignoring the byte order of the given ByteBuffer.  If the capacity of the
   * given ByteBuffer is zero the endianness of the returned <i>Memory</i> object (as well as backing
   * storage) is unspecified.
   * @param byteBuf the given ByteBuffer, must not be null
   * @param dataByteOrder the byte order of the uderlying data independent of the byte order
   * state of the given ByteBuffer
   * @return a new <i>Memory</i> for read-only operations on the given ByteBuffer.
   */
  public static Memory wrap(final ByteBuffer byteBuf, final ByteOrder dataByteOrder) {
    return BaseWritableMemoryImpl.wrapByteBuffer(byteBuf, true, dataByteOrder);
  }

  //MAP XXX
  /**
   * Allocates direct memory used to memory map entire files for read operations
   * (including those &gt; 2GB). This assumes that the file was written using native byte
   * ordering.
   * @param file the given file to map
   * @return <i>MapHandle</i> for managing this map
   * @throws IOException if file not found or internal RuntimeException is thrown.
   */
  public static MapHandle map(final File file) throws IOException {
    return map(file, 0, file.length(), ByteOrder.nativeOrder());
  }

  /**
   * Allocates direct memory used to memory map files for read operations
   * (including those &gt; 2GB).
   * @param file the given file to map. It may not be null.
   * @param fileOffsetBytes the position in the given file in bytes. It may not be negative.
   * @param capacityBytes the size of the allocated direct memory. It may not be negative or zero.
   * @param dataByteOrder the endianness of the given file. It may not be null.
   * @return <i>MapHandle</i> for managing this map
   * @throws IOException if file not found or internal RuntimeException is thrown.
   */
  public static MapHandle map(final File file, final long fileOffsetBytes, final long capacityBytes,
      final ByteOrder dataByteOrder) throws IOException {
    zeroCheck(capacityBytes, "Capacity");
    nullCheck(file, "file is null");
    negativeCheck(fileOffsetBytes, "File offset is negative");
    return BaseWritableMemoryImpl
        .wrapMap(file, fileOffsetBytes, capacityBytes, true, dataByteOrder);
  }

  //REGIONS XXX
  /**
   * A region is a read-only view of the backing store of this object.
   * This returns a new <i>Memory</i> representing the defined region.
   * <ul>
   * <li>Returned object's origin = this object's origin + offsetBytes</li>
   * <li>Returned object's capacity = capacityBytes</li>
   * </ul>
   * If the given capacityBytes is zero, the returned object is effectively immutable and
   * the backing storage and endianness are unspecified.
   * @param offsetBytes the starting offset with respect to the origin of this Memory.
   * @param capacityBytes the capacity of the region in bytes
   * @return a new <i>Memory</i> representing the defined region.
   */
  public abstract Memory region(long offsetBytes, long capacityBytes);

  //AS BUFFER XXX
  /**
   * Returns a new <i>Buffer</i> view of the backing store of this object..
   * <ul>
   * <li>Returned object's origin = this object's origin</li>
   * <li>Returned object's <i>start</i> = 0</li>
   * <li>Returned object's <i>position</i> = 0</li>
   * <li>Returned object's <i>end</i> = this object's capacity</li>
   * <li>Returned object's <i>capacity</i> = this object's capacity</li>
   * <li>Returned object's <i>start</i>, <i>position</i> and <i>end</i> are mutable</li>
   * </ul>
   * If this object's capacity is zero, the returned object is effectively immutable and
   * the backing storage and endianness are unspecified.
   * @return a new <i>Buffer</i>
   */
  public abstract Buffer asBuffer();

  //ACCESS PRIMITIVE HEAP ARRAYS for readOnly XXX
  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final boolean[] arr) {
    final long lengthBytes = arr.length << Prim.BOOLEAN.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final byte[] arr) {
    return Memory.wrap(arr, 0, arr.length, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations with the given byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @param dataByteOrder the byte order
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final byte[] arr, final ByteOrder dataByteOrder) {
    return Memory.wrap(arr, 0, arr.length, dataByteOrder);
  }

  /**
   * Wraps the given primitive array for read operations with the given byte order. If the given
   * lengthBytes is zero, backing storage and endianness of the returned <i>Memory</i> object are
   * unspecified.
   * @param arr the given primitive array.
   * @param offsetBytes the byte offset into the given array
   * @param lengthBytes the number of bytes to include from the given array
   * @param dataByteOrder the byte order
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final byte[] arr, final int offsetBytes, final int lengthBytes,
      final ByteOrder dataByteOrder) {
    UnsafeUtil.checkBounds(offsetBytes, lengthBytes, arr.length);
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, dataByteOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final char[] arr) {
    final long lengthBytes = arr.length << Prim.CHAR.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final short[] arr) {
    final long lengthBytes = arr.length << Prim.SHORT.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final int[] arr) {
    final long lengthBytes = arr.length << Prim.INT.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final long[] arr) {
    final long lengthBytes = arr.length << Prim.LONG.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final float[] arr) {
    final long lengthBytes = arr.length << Prim.FLOAT.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  /**
   * Wraps the given primitive array for read operations assuming native byte order. If the array
   * size is zero, backing storage and endianness of the returned <i>Memory</i> object are unspecified.
   * @param arr the given primitive array.
   * @return a new <i>Memory</i> for read operations
   */
  public static Memory wrap(final double[] arr) {
    final long lengthBytes = arr.length << Prim.DOUBLE.shift();
    return BaseWritableMemoryImpl.wrapHeapArray(arr, 0L, lengthBytes, true, nativeOrder);
  }

  //PRIMITIVE getXXX() and getXXXArray() XXX
  /**
   * Gets the boolean value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the boolean at the given offset
   */
  public abstract boolean getBoolean(long offsetBytes);

  /**
   * Gets the boolean array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetBooleans offset in array units
   * @param lengthBooleans number of array units to transfer
   */
  public abstract void getBooleanArray(long offsetBytes, boolean[] dstArray, int dstOffsetBooleans,
      int lengthBooleans);

  /**
   * Gets the byte value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the byte at the given offset
   */
  public abstract byte getByte(long offsetBytes);

  /**
   * Gets the byte array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetBytes offset in array units
   * @param lengthBytes number of array units to transfer
   */
  public abstract void getByteArray(long offsetBytes, byte[] dstArray, int dstOffsetBytes,
      int lengthBytes);

  /**
   * Gets the char value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the char at the given offset
   */
  public abstract char getChar(long offsetBytes);

  /**
   * Gets the char array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetChars offset in array units
   * @param lengthChars number of array units to transfer
   */
  public abstract void getCharArray(long offsetBytes, char[] dstArray, int dstOffsetChars,
      int lengthChars);

  /**
   * Gets UTF-8 encoded bytes from this Memory, starting at offsetBytes to a length of
   * utf8LengthBytes, decodes them into characters and appends them to the given Appendable.
   * This is specifically designed to reduce the production of intermediate objects (garbage),
   * thus significantly reducing pressure on the JVM Garbage Collector.
   * @param offsetBytes offset bytes relative to the Memory start
   * @param utf8LengthBytes the number of encoded UTF-8 bytes to decode. It is assumed that the
   * caller has the correct number of utf8 bytes required to decode the number of characters
   * to be appended to dst. Characters outside the ASCII range can require 2, 3 or 4 bytes per
   * character to decode.
   * @param dst the destination Appendable to append the decoded characters to.
   * @return the number of characters decoded
   * @throws IOException if dst.append() throws IOException
   * @throws Utf8CodingException in case of malformed or illegal UTF-8 input
   */
  public abstract int getCharsFromUtf8(long offsetBytes, int utf8LengthBytes, Appendable dst)
      throws IOException, Utf8CodingException;

  /**
   * Gets UTF-8 encoded bytes from this Memory, starting at offsetBytes to a length of
   * utf8LengthBytes, decodes them into characters and appends them to the given StringBuilder.
   * This method does *not* reset the length of the destination StringBuilder before appending
   * characters to it.
   * This is specifically designed to reduce the production of intermediate objects (garbage),
   * thus significantly reducing pressure on the JVM Garbage Collector.
   * @param offsetBytes offset bytes relative to the Memory start
   * @param utf8LengthBytes the number of encoded UTF-8 bytes to decode. It is assumed that the
   * caller has the correct number of utf8 bytes required to decode the number of characters
   * to be appended to dst. Characters outside the ASCII range can require 2, 3 or 4 bytes per
   * character to decode.
   * @param dst the destination StringBuilder to append decoded characters to.
   * @return the number of characters decoded.
   * @throws Utf8CodingException in case of malformed or illegal UTF-8 input
   */
  public abstract int getCharsFromUtf8(final long offsetBytes, final int utf8LengthBytes,
      final StringBuilder dst) throws Utf8CodingException;

  /**
   * Gets the double value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the double at the given offset
   */
  public abstract double getDouble(long offsetBytes);

  /**
   * Gets the double array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetDoubles offset in array units
   * @param lengthDoubles number of array units to transfer
   */
  public abstract void getDoubleArray(long offsetBytes, double[] dstArray, int dstOffsetDoubles,
      int lengthDoubles);

  /**
   * Gets the float value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the float at the given offset
   */
  public abstract float getFloat(long offsetBytes);

  /**
   * Gets the float array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetFloats offset in array units
   * @param lengthFloats number of array units to transfer
   */
  public abstract void getFloatArray(long offsetBytes, float[] dstArray, int dstOffsetFloats,
      int lengthFloats);

  /**
   * Gets the int value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the int at the given offset
   */
  public abstract int getInt(long offsetBytes);

  /**
   * Gets the int array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetInts offset in array units
   * @param lengthInts number of array units to transfer
   */
  public abstract void getIntArray(long offsetBytes, int[] dstArray, int dstOffsetInts,
      int lengthInts);

  /**
   * Gets the long value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the long at the given offset
   */
  public abstract long getLong(long offsetBytes);

  /**
   * Gets the long array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetLongs offset in array units
   * @param lengthLongs number of array units to transfer
   */
  public abstract void getLongArray(long offsetBytes, long[] dstArray, int dstOffsetLongs,
      int lengthLongs);

  /**
   * Gets the short value at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @return the short at the given offset
   */
  public abstract short getShort(long offsetBytes);

  /**
   * Gets the short array at the given offset
   * @param offsetBytes offset bytes relative to this Memory start
   * @param dstArray The preallocated destination array.
   * @param dstOffsetShorts offset in array units
   * @param lengthShorts number of array units to transfer
   */
  public abstract void getShortArray(long offsetBytes, short[] dstArray, int dstOffsetShorts,
      int lengthShorts);

  //SPECIAL PRIMITIVE READ METHODS: compareTo, copyTo, writeTo XXX
  /**
   * Compares the bytes of this Memory to <i>that</i> Memory.
   * Returns <i>(this &lt; that) ? (some negative value) : (this &gt; that) ? (some positive value)
   * : 0;</i>.
   * If all bytes are equal up to the shorter of the two lengths, the shorter length is considered
   * to be less than the other.
   * @param thisOffsetBytes the starting offset for <i>this Memory</i>
   * @param thisLengthBytes the length of the region to compare from <i>this Memory</i>
   * @param that the other Memory to compare with
   * @param thatOffsetBytes the starting offset for <i>that Memory</i>
   * @param thatLengthBytes the length of the region to compare from <i>that Memory</i>
   * @return <i>(this &lt; that) ? (some negative value) : (this &gt; that) ? (some positive value)
   * : 0;</i>
   */
  public abstract int compareTo(long thisOffsetBytes, long thisLengthBytes, Memory that,
      long thatOffsetBytes, long thatLengthBytes);

  /**
   * Copies bytes from a source range of this Memory to a destination range of the given Memory
   * with the same semantics when copying between overlapping ranges of bytes as method
   * {@link java.lang.System#arraycopy(Object, int, Object, int, int)} has. However, if the source
   * and the destination ranges are exactly the same, this method throws {@link
   * IllegalArgumentException}, because it should never be needed in real-world scenarios and
   * therefore indicates a bug.
   * @param srcOffsetBytes the source offset for this Memory
   * @param destination the destination Memory, which may not be Read-Only.
   * @param dstOffsetBytes the destination offset
   * @param lengthBytes the number of bytes to copy
   */
  public abstract void copyTo(long srcOffsetBytes, WritableMemory destination, long dstOffsetBytes,
      long lengthBytes);

  /**
   * Writes bytes from a source range of this Memory to the given {@code WritableByteChannel}.
   * @param offsetBytes the source offset for this Memory
   * @param lengthBytes the number of bytes to copy
   * @param out the destination WritableByteChannel
   * @throws IOException may occur while writing to the WritableByteChannel
   */
  public abstract void writeTo(long offsetBytes, long lengthBytes, WritableByteChannel out)
      throws IOException;

}
