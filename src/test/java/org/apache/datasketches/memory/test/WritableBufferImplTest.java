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

package org.apache.datasketches.memory.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.internal.Buffer;
import org.apache.datasketches.memory.internal.MemoryImpl;
import org.apache.datasketches.memory.internal.ReadOnlyException;
import org.apache.datasketches.memory.internal.UnsafeUtil;
import org.apache.datasketches.memory.internal.Util;
import org.apache.datasketches.memory.internal.WritableBuffer;
import org.apache.datasketches.memory.internal.WritableMemoryImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class WritableBufferImplTest {

  //Simple Native direct

  @SuppressWarnings("resource")
  @Test
  public void checkNativeCapacityAndClose() {
    int memCapacity = 64;
    WritableHandle wmh = WritableMemoryImpl.allocateDirect(memCapacity);
    WritableMemoryImpl wmem = wmh.get();
    WritableBuffer wbuf = wmem.asWritableBuffer();
    assertEquals(wbuf.getCapacity(), memCapacity);

    wmh.close(); //intentional
    assertFalse(wbuf.isValid());

    wmh.close(); //intentional, nothing to free
  }

  //Simple Heap arrays

  @Test
  public void checkBooleanArray() {
    boolean[] srcArray = { true, false, true, false, false, true, true, false };
    boolean[] dstArray = new boolean[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getBooleanArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getBooleanArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
    assertTrue(buf.hasArray());
  }

  @Test
  public void checkByteArray() {
    byte[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    byte[] dstArray = new byte[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getByteArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getByteArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkCharArray() {
    char[] srcArray = { 1, 2, 3, 4, 5, 6, 7, 8 };
    char[] dstArray = new char[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getCharArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getCharArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkShortArray() {
    short[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    short[] dstArray = new short[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getShortArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getShortArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkIntArray() {
    int[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    int[] dstArray = new int[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getIntArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getIntArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkLongArray() {
    long[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    long[] dstArray = new long[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getLongArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getLongArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkFloatArray() {
    float[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    float[] dstArray = new float[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getFloatArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getFloatArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkDoubleArray() {
    double[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };
    double[] dstArray = new double[8];

    Buffer buf = MemoryImpl.wrap(srcArray).asBuffer();
    buf.getDoubleArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    wbuf.getDoubleArray(dstArray, 0, 8);
    for (int i=0; i<8; i++) {
      assertEquals(dstArray[i], srcArray[i]);
    }
  }

  @Test
  public void checkNativeBaseBound() {
    int memCapacity = 64;
    try (WritableHandle wrh = WritableMemoryImpl.allocateDirect(memCapacity)) {
      WritableMemoryImpl wmem = wrh.get();
      WritableBuffer wbuf = wmem.asWritableBuffer();
      wbuf.toHexString("Force Assertion Error", memCapacity, 8);
    } catch (IllegalArgumentException e) {
      //ok
    }
  }

  @Test
  public void checkNativeSrcArrayBound() {
    long memCapacity = 64;
    try (WritableHandle wrh = WritableMemoryImpl.allocateDirect(memCapacity)) {
      WritableMemoryImpl wmem = wrh.get();
      WritableBuffer wbuf = wmem.asWritableBuffer();
      byte[] srcArray = { 1, -2, 3, -4 };
      wbuf.putByteArray(srcArray, 0, 5); //wrong!
    } catch (IllegalArgumentException e) {
      //pass
    }
  }


  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkRegionBounds() {
    int memCapacity = 64;
    try (WritableHandle wrh = WritableMemoryImpl.allocateDirect(memCapacity)) {
      WritableMemoryImpl wmem = wrh.get();
      WritableBuffer wbuf = wmem.asWritableBuffer();
      wbuf.writableRegion(1, 64, wbuf.getTypeByteOrder()); //wrong!
    }
  }

  @Test
  public void checkByteBufferWrap() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocate(memCapacity);
    byteBuf.order(ByteOrder.nativeOrder());

    for (int i=0; i<memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }

    WritableBuffer wbuf = WritableBuffer.wrap(byteBuf);

    for (int i=0; i<memCapacity; i++) {
      assertEquals(wbuf.getByte(), byteBuf.get(i));
    }

    assertTrue(wbuf.hasByteBuffer());
    ByteBuffer byteBuf2 = wbuf.getByteBuffer();
    assertEquals(byteBuf2, byteBuf);
    //println( mem.toHexString("HeapBB", 0, memCapacity));
  }

  @Test
  public void checkWrapWithBBReadonly1() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocate(memCapacity);
    byteBuf.order(ByteOrder.nativeOrder());

    for (int i = 0; i < memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }

    Buffer buf = WritableBuffer.wrap(byteBuf);

    for (int i = 0; i < memCapacity; i++) {
      assertEquals(buf.getByte(), byteBuf.get(i));
    }

    //println(mem.toHexString("HeapBB", 0, memCapacity));
  }

  @Test(expectedExceptions = ReadOnlyException.class)
  public void checkWrapWithBBReadonly2() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocate(memCapacity);
    byteBuf.order(ByteOrder.nativeOrder());
    ByteBuffer byteBufRO = byteBuf.asReadOnlyBuffer();

    WritableBuffer.wrap(byteBufRO);
  }

  @Test
  public void checkWrapWithDirectBBReadonly() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocateDirect(memCapacity);
    byteBuf.order(ByteOrder.nativeOrder());

    for (int i = 0; i < memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }
    ByteBuffer byteBufRO = byteBuf.asReadOnlyBuffer();
    byteBufRO.order(ByteOrder.nativeOrder());

    Buffer buf = Buffer.wrap(byteBufRO);

    for (int i = 0; i < memCapacity; i++) {
      assertEquals(buf.getByte(), byteBuf.get(i));
    }

    //println(mem.toHexString("HeapBB", 0, memCapacity));
  }

  @Test(expectedExceptions = ReadOnlyException.class)
  public void checkWrapWithDirectBBReadonlyPut() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocateDirect(memCapacity);
    ByteBuffer byteBufRO = byteBuf.asReadOnlyBuffer();
    byteBufRO.order(ByteOrder.nativeOrder());

    WritableBuffer.wrap(byteBufRO);
  }

  @Test
  public void checkByteBufferWrapDirectAccess() {
    int memCapacity = 64;
    ByteBuffer byteBuf = ByteBuffer.allocateDirect(memCapacity);
    byteBuf.order(ByteOrder.nativeOrder());

    for (int i=0; i<memCapacity; i++) {
      byteBuf.put(i, (byte) i);
    }

    Buffer buf = Buffer.wrap(byteBuf);

    for (int i=0; i<memCapacity; i++) {
      assertEquals(buf.getByte(), byteBuf.get(i));
    }

    //println( mem.toHexString("HeapBB", 0, memCapacity));
  }

  @Test
  public void checkIsDirect() {
    int memCapacity = 64;
    WritableBuffer mem = WritableMemoryImpl.allocate(memCapacity).asWritableBuffer();
    assertFalse(mem.isDirect());
    try (WritableHandle wrh = WritableMemoryImpl.allocateDirect(memCapacity)) {
      WritableMemoryImpl mem2 = wrh.get();
      WritableBuffer wbuf = mem2.asWritableBuffer();
      assertTrue(wbuf.isDirect());
      wrh.close();
    }
  }

  @Test
  public void checkIsReadOnly() {
    long[] srcArray = { 1, -2, 3, -4, 5, -6, 7, -8 };

    WritableBuffer wbuf = WritableMemoryImpl.writableWrap(srcArray).asWritableBuffer();
    assertFalse(wbuf.isReadOnly());

    Buffer buf = wbuf;
    assertFalse(buf.isReadOnly());

    for (int i = 0; i < srcArray.length; i++) {
      assertEquals(buf.getLong(), srcArray[i]);
    }
  }

  @Test
  public void checkGoodBounds() {
    UnsafeUtil.checkBounds(50, 50, 100);
  }

  @Test
  public void checkCompareToHeap() {
    byte[] arr1 = new byte[] {0, 1, 2, 3};
    byte[] arr2 = new byte[] {0, 1, 2, 4};
    byte[] arr3 = new byte[] {0, 1, 2, 3, 4};

    Buffer buf1 = MemoryImpl.wrap(arr1).asBuffer();
    Buffer buf2 = MemoryImpl.wrap(arr2).asBuffer();
    Buffer buf3 = MemoryImpl.wrap(arr3).asBuffer();

    int comp = buf1.compareTo(0, 3, buf2, 0, 3);
    assertEquals(comp, 0);
    comp = buf1.compareTo(0, 4, buf2, 0, 4);
    assertEquals(comp, -1);
    comp = buf2.compareTo(0, 4, buf1, 0, 4);
    assertEquals(comp, 1);
    //different lengths
    comp = buf1.compareTo(0, 4, buf3, 0, 5);
    assertEquals(comp, -1);
    comp = buf3.compareTo(0, 5, buf1, 0, 4);
    assertEquals(comp, 1);
  }

  @Test
  public void checkCompareToDirect() {
    byte[] arr1 = new byte[] {0, 1, 2, 3};
    byte[] arr2 = new byte[] {0, 1, 2, 4};
    byte[] arr3 = new byte[] {0, 1, 2, 3, 4};

    try (WritableHandle h1 = WritableMemoryImpl.allocateDirect(4);
        WritableHandle h2 = WritableMemoryImpl.allocateDirect(4);
        WritableHandle h3 = WritableMemoryImpl.allocateDirect(5))
    {
      WritableMemoryImpl mem1 = h1.get();
      mem1.putByteArray(0, arr1, 0, 4);

      WritableMemoryImpl mem2 = h2.get();
      mem2.putByteArray(0, arr2, 0, 4);

      WritableMemoryImpl mem3 = h3.get();
      mem3.putByteArray(0, arr3, 0, 5);

      Buffer buf1 = mem1.asBuffer();
      Buffer buf2 = mem2.asBuffer();
      Buffer buf3 = mem3.asBuffer();

      int comp = buf1.compareTo(0, 3, buf2, 0, 3);
      assertEquals(comp, 0);
      comp = buf1.compareTo(0, 4, buf2, 0, 4);
      assertEquals(comp, -1);
      comp = buf2.compareTo(0, 4, buf1, 0, 4);
      assertEquals(comp, 1);
      //different lengths
      comp = buf1.compareTo(0, 4, buf3, 0, 5);
      assertEquals(comp, -1);
      comp = buf3.compareTo(0, 5, buf1, 0, 4);
      assertEquals(comp, 1);
    }
  }

  @Test
  public void checkAsBuffer() {
    WritableMemoryImpl wmem = WritableMemoryImpl.allocate(64);
    WritableBuffer wbuf = wmem.asWritableBuffer();
    wbuf.setPosition(32);
    for (int i = 32; i < 64; i++) { wbuf.putByte((byte)i); }
    //println(wbuf.toHexString("Buf", 0, (int)wbuf.getCapacity()));

    Buffer buf = wmem.asBuffer();
    buf.setPosition(32);
    for (int i = 32; i < 64; i++) {
      assertEquals(buf.getByte(), i);
    }
  }

  @Test
  public void checkDuplicate() {
    WritableMemoryImpl wmem = WritableMemoryImpl.allocate(64);
    for (int i = 0; i < 64; i++) { wmem.putByte(i, (byte)i); }

    WritableBuffer wbuf = wmem.asWritableBuffer().writableDuplicate();
    wbuf.checkValidAndBounds(0, 64);
    for (int i = 0; i < 64; i++) {
      assertEquals(wbuf.getByte(), i);
    }
    Buffer buf = wmem.asBuffer().duplicate();
    for (int i = 0; i < 64; i++) {
      assertEquals(buf.getByte(), i);
    }

    WritableMemoryImpl wmem2 = wbuf.asWritableMemory();
    for (int i = 0; i < 64; i++) {
      assertEquals(wmem2.getByte(i), i);
    }
    WritableMemoryImpl wmem3 = wbuf.asWritableMemory();
    wmem3.checkValidAndBounds(0, 64);
  }

  @Test
  public void checkCumAndRegionOffset() {
    WritableMemoryImpl wmem = WritableMemoryImpl.allocate(64);
    WritableMemoryImpl reg = wmem.writableRegion(32, 32);
    WritableBuffer buf = reg.asWritableBuffer();
    assertEquals(buf.getRegionOffset(), 32);
    assertEquals(buf.getRegionOffset(0), 32);
    assertEquals(buf.getCumulativeOffset(), 32 + 16);
    assertEquals(buf.getCumulativeOffset(0), 32 + 16);
  }

  @Test
  public void checkIsSameResource() {
    byte[] byteArr = new byte[64];
    WritableBuffer wbuf1 = WritableMemoryImpl.writableWrap(byteArr).asWritableBuffer();
    WritableBuffer wbuf2 = WritableMemoryImpl.writableWrap(byteArr).asWritableBuffer();
    assertTrue(wbuf1.isSameResource(wbuf2));
  }

  @Test
  public void checkDegenerateRegionReturn() {
    MemoryImpl mem = MemoryImpl.wrap(new byte[0]);
    Buffer buf = mem.asBuffer();
    Buffer reg = buf.region();
    assertEquals(reg.getCapacity(), 0);
  }

  @Test
  public void checkAsWritableMemoryRO() {
    ByteBuffer bb = ByteBuffer.allocate(64);
    WritableBuffer wbuf = WritableBuffer.wrap(bb);
    @SuppressWarnings("unused")
    WritableMemoryImpl wmem = wbuf.asWritableMemory();

    try {
      Buffer buf = Buffer.wrap(bb);
      wbuf = (WritableBuffer) buf;
      @SuppressWarnings("unused")
      WritableMemoryImpl wmem2 = wbuf.asWritableMemory();
      Assert.fail();
    } catch (ReadOnlyException expected) {
      // expected
    }
  }

  @Test
  public void checkWritableDuplicateRO() {
    ByteBuffer bb = ByteBuffer.allocate(64);
    WritableBuffer wbuf = WritableBuffer.wrap(bb);
    @SuppressWarnings("unused")
    WritableBuffer wdup = wbuf.writableDuplicate();

    try {
      Buffer buf = Buffer.wrap(bb);
      wbuf = (WritableBuffer) buf;
      @SuppressWarnings("unused")
      WritableBuffer wdup2 = wbuf.writableDuplicate();
      Assert.fail();
    } catch (ReadOnlyException expected) {
      // ignore
    }
  }

  @Test
  public void checkWritableRegionRO() {
    ByteBuffer bb = ByteBuffer.allocate(64);
    WritableBuffer wbuf = WritableBuffer.wrap(bb);
    @SuppressWarnings("unused")
    WritableBuffer wreg = wbuf.writableRegion();

    try {
      Buffer buf = Buffer.wrap(bb);
      wbuf = (WritableBuffer) buf;
      @SuppressWarnings("unused")
      WritableBuffer wreg2 = wbuf.writableRegion();
      Assert.fail();
    } catch (ReadOnlyException expected) {
      // ignore
    }
  }

  @Test
  public void checkWritableRegionWithParamsRO() {
    ByteBuffer bb = ByteBuffer.allocate(64);
    WritableBuffer wbuf = WritableBuffer.wrap(bb);
    @SuppressWarnings("unused")
    WritableBuffer wreg = wbuf.writableRegion(0, 1, wbuf.getTypeByteOrder());

    try {
      Buffer buf = Buffer.wrap(bb);
      wbuf = (WritableBuffer) buf;
      @SuppressWarnings("unused")
      WritableBuffer wreg2 = wbuf.writableRegion(0, 1, wbuf.getTypeByteOrder());
      Assert.fail();
    } catch (ReadOnlyException expected) {
      // ignore
    }
  }

  @Test
  public void checkZeroBuffer() {
    WritableMemoryImpl wmem = WritableMemoryImpl.allocate(8);
    WritableBuffer wbuf = wmem.asWritableBuffer();
    WritableBuffer reg = wbuf.writableRegion(0, 0, wbuf.getTypeByteOrder());
    assertEquals(reg.getCapacity(), 0);
  }

  @Test
  public void checkDuplicateNonNative() {
    WritableMemoryImpl wmem = WritableMemoryImpl.allocate(64);
    wmem.putShort(0, (short) 1);
    Buffer buf = wmem.asWritableBuffer().duplicate(Util.nonNativeByteOrder);
    assertEquals(buf.getShort(0), 256);
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  /**
   * @param s value to print
   */
  static void println(String s) {
    //System.out.println(s); //disable here
  }

}
