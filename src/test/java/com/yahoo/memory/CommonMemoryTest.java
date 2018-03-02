/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.memory;

import static com.yahoo.memory.Util.isAllBitsClear;
import static com.yahoo.memory.Util.isAllBitsSet;
import static com.yahoo.memory.Util.isAnyBitsClear;
import static com.yahoo.memory.Util.isAnyBitsSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class CommonMemoryTest {

  @Test
  public void checkSetGet() {
    int memCapacity = 16; //must be at least 8
    try (WritableHandle wrh = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh.get();
      assertEquals(mem.getCapacity(), memCapacity);
      setGetTests(mem);
    }
  }

  public static void setGetTests(WritableMemory mem) {
    mem.putBoolean(0, true);
    assertEquals(mem.getBoolean(0), true);
    mem.putBoolean(0, false);
    assertEquals(mem.getBoolean(0), false);

    mem.putByte(0, (byte) -1);
    assertEquals(mem.getByte(0), (byte) -1);
    mem.putByte(0, (byte)0);
    assertEquals(mem.getByte(0), (byte)0);

    mem.putChar(0, 'A');
    assertEquals(mem.getChar(0), 'A');
    mem.putChar(0, 'Z');
    assertEquals(mem.getChar(0), 'Z');

    mem.putShort(0, Short.MAX_VALUE);
    assertEquals(mem.getShort(0), Short.MAX_VALUE);
    mem.putShort(0, Short.MIN_VALUE);
    assertEquals(mem.getShort(0), Short.MIN_VALUE);

    mem.putInt(0, Integer.MAX_VALUE);
    assertEquals(mem.getInt(0), Integer.MAX_VALUE);
    mem.putInt(0, Integer.MIN_VALUE);
    assertEquals(mem.getInt(0), Integer.MIN_VALUE);

    mem.putFloat(0, Float.MAX_VALUE);
    assertEquals(mem.getFloat(0), Float.MAX_VALUE);
    mem.putFloat(0, Float.MIN_VALUE);
    assertEquals(mem.getFloat(0), Float.MIN_VALUE);

    mem.putLong(0, Long.MAX_VALUE);
    assertEquals(mem.getLong(0), Long.MAX_VALUE);
    mem.putLong(0, Long.MIN_VALUE);
    assertEquals(mem.getLong(0), Long.MIN_VALUE);

    mem.putDouble(0, Double.MAX_VALUE);
    assertEquals(mem.getDouble(0), Double.MAX_VALUE);
    mem.putDouble(0, Double.MIN_VALUE);
    assertEquals(mem.getDouble(0), Double.MIN_VALUE);
  }

  @Test
  public void checkSetGetArrays() {
    int memCapacity = 32;
    try (WritableHandle wrh = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh.get();
      assertEquals(memCapacity, mem.getCapacity());
      setGetArraysTests(mem);
    }
  }

  public static void setGetArraysTests(WritableMemory mem) {
    int accessCapacity = (int)mem.getCapacity();

    int words = 4;
    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[words];
    mem.fill(0, accessCapacity, (byte)127);
    mem.putBooleanArray(0, srcArray1, 0, words);
    mem.getBooleanArray(0, dstArray1, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = { 1, -2, 3, -4 };
    byte[] dstArray2 = new byte[4];
    mem.putByteArray(0, srcArray2, 0, words);
    mem.getByteArray(0, dstArray2, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }

    char[] srcArray3 = { 'A', 'B', 'C', 'D' };
    char[] dstArray3 = new char[words];
    mem.putCharArray(0, srcArray3, 0, words);
    mem.getCharArray(0, dstArray3, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }

    double[] srcArray4 = { 1.0, -2.0, 3.0, -4.0 };
    double[] dstArray4 = new double[words];
    mem.putDoubleArray(0, srcArray4, 0, words);
    mem.getDoubleArray(0, dstArray4, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }

    float[] srcArray5 = { (float)1.0, (float)-2.0, (float)3.0, (float)-4.0 };
    float[] dstArray5 = new float[words];
    mem.putFloatArray(0, srcArray5, 0, words);
    mem.getFloatArray(0, dstArray5, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }

    int[] srcArray6 = { 1, -2, 3, -4 };
    int[] dstArray6 = new int[words];
    mem.putIntArray(0, srcArray6, 0, words);
    mem.getIntArray(0, dstArray6, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }

    long[] srcArray7 = { 1, -2, 3, -4 };
    long[] dstArray7 = new long[words];
    mem.putLongArray(0, srcArray7, 0, words);
    mem.getLongArray(0, dstArray7, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }

    short[] srcArray8 = { 1, -2, 3, -4 };
    short[] dstArray8 = new short[words];
    mem.putShortArray(0, srcArray8, 0, words);
    mem.getShortArray(0, dstArray8, 0, words);
    for (int i=0; i<words; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }

  @Test
  public void checkSetGetPartialArraysWithOffset() {
    int memCapacity = 32;
    try (WritableHandle wrh = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh.get();
      assertEquals(memCapacity, mem.getCapacity());
      setGetPartialArraysWithOffsetTests(mem);
    }
  }

  public static void setGetPartialArraysWithOffsetTests(WritableMemory mem) {
    int items= 4;
    boolean[] srcArray1 = {true, false, true, false};
    boolean[] dstArray1 = new boolean[items];
    mem.putBooleanArray(0, srcArray1, 2, items/2);
    mem.getBooleanArray(0, dstArray1, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray1[i], srcArray1[i]);
    }

    byte[] srcArray2 = { 1, -2, 3, -4 };
    byte[] dstArray2 = new byte[items];
    mem.putByteArray(0, srcArray2, 2, items/2);
    mem.getByteArray(0, dstArray2, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray2[i], srcArray2[i]);
    }

    char[] srcArray3 = { 'A', 'B', 'C', 'D' };
    char[] dstArray3 = new char[items];
    mem.putCharArray(0, srcArray3, 2, items/2);
    mem.getCharArray(0, dstArray3, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray3[i], srcArray3[i]);
    }

    double[] srcArray4 = { 1.0, -2.0, 3.0, -4.0 };
    double[] dstArray4 = new double[items];
    mem.putDoubleArray(0, srcArray4, 2, items/2);
    mem.getDoubleArray(0, dstArray4, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray4[i], srcArray4[i], 0.0);
    }

    float[] srcArray5 = { (float)1.0, (float)-2.0, (float)3.0, (float)-4.0 };
    float[] dstArray5 = new float[items];
    mem.putFloatArray(0, srcArray5, 2, items/2);
    mem.getFloatArray(0, dstArray5, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray5[i], srcArray5[i], 0.0);
    }

    int[] srcArray6 = { 1, -2, 3, -4 };
    int[] dstArray6 = new int[items];
    mem.putIntArray(0, srcArray6, 2, items/2);
    mem.getIntArray(0, dstArray6, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray6[i], srcArray6[i]);
    }

    long[] srcArray7 = { 1, -2, 3, -4 };
    long[] dstArray7 = new long[items];
    mem.putLongArray(0, srcArray7, 2, items/2);
    mem.getLongArray(0, dstArray7, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray7[i], srcArray7[i]);
    }

    short[] srcArray8 = { 1, -2, 3, -4 };
    short[] dstArray8 = new short[items];
    mem.putShortArray(0, srcArray8, 2, items/2);
    mem.getShortArray(0, dstArray8, 2, items/2);
    for (int i=2; i<items; i++) {
      assertEquals(dstArray8[i], srcArray8[i]);
    }
  }

  @Test
  public void checkSetClearIsBits() {
    int memCapacity = 8;
    try (WritableHandle wrh = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh.get();
      assertEquals(memCapacity, mem.getCapacity());
      mem.clear();
      setClearIsBitsTests(mem);
    }
  }

  public static void setClearIsBitsTests(WritableMemory mem) {
  //single bits
    for (int i=0; i<8; i++) {
      long bitMask = (1 << i);
      long v = mem.getByte(0) & 0XFFL;
      assertTrue(isAnyBitsClear(v, bitMask));
      mem.setBits(0, (byte) bitMask);
      v = mem.getByte(0) & 0XFFL;
      assertTrue(isAnyBitsSet(v, bitMask));
      mem.clearBits(0, (byte) bitMask);
      v = mem.getByte(0) & 0XFFL;
      assertTrue(isAnyBitsClear(v, bitMask));
    }

    //multiple bits
    for (int i=0; i<7; i++) {
      long bitMask1 = (1 << i);
      long bitMask2 = (3 << i);
      long v = mem.getByte(0) & 0XFFL;
      assertTrue(isAnyBitsClear(v, bitMask1));
      assertTrue(isAnyBitsClear(v, bitMask2));
      mem.setBits(0, (byte) bitMask1); //set one bit
      v = mem.getByte(0) & 0XFFL;
      assertTrue(isAnyBitsSet(v, bitMask2));
      assertTrue(isAnyBitsClear(v, bitMask2));
      assertFalse(isAllBitsSet(v, bitMask2));
      assertFalse(isAllBitsClear(v, bitMask2));
    }
  }

  @Test
  public void checkAtomicMethods() {
    int memCapacity = 8;
    try (WritableHandle wrh = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh.get();
      assertEquals(mem.getCapacity(), memCapacity);
      atomicMethodTests(mem);
    }
  }

  public static void atomicMethodTests(WritableMemory mem) {
    mem.putLong(0, 500);
    mem.getAndAddLong(0, 1);
    assertEquals(mem.getLong(0), 501);

    mem.putInt(0, 500);
    boolean b = mem.compareAndSwapLong(0, 500, 501);
    assertTrue(b);
    assertEquals(mem.getLong(0), 501);

    mem.putLong(0, 500);
    long oldLong = mem.getAndSetLong(0, 501);
    long newLong = mem.getLong(0);
    assertEquals(oldLong, 500);
    assertEquals(newLong, 501);
  }

  @Test
  public void checkSetClearMemoryRegions() {
    int memCapacity = 64; //must be 64
    try (WritableHandle wrh1 = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh1.get();

      setClearMemoryRegionsTests(mem); //requires println enabled to visually check
      for (int i = 0; i < memCapacity; i++) {
        assertEquals(mem.getByte(i), 0);
      }
    }
  }

  //enable println stmts to visually check
  public static void setClearMemoryRegionsTests(WritableMemory mem) {
    int accessCapacity = (int)mem.getCapacity();

  //define regions
    int reg1Start = 0;
    int reg1Len = 28;
    int reg2Start = 28;
    int reg2Len = 32;

    //set region 1
    byte b1 = 5;
    mem.fill(reg1Start, reg1Len, b1);
    for (int i=reg1Start; i<reg1Len+reg1Start; i++) {
      assertEquals(mem.getByte(i), b1);
    }
    //println(mem.toHexString("Region1 to 5", reg1Start, reg1Len));

    //set region 2
    byte b2 = 7;
    mem.fill(reg2Start, reg2Len, b2);
    //println(mem.toHexString("Fill", 0, (int)mem.getCapacity()));
    for (int i=reg2Start; i<reg2Len+reg2Start; i++) {
      assertEquals(mem.getByte(i), b2);
    }
    //println(mem.toHexString("Region2 to 7", reg2Start, reg2Len));

    //clear region 1
    byte zeroByte = 0;
    mem.clear(reg1Start, reg1Len);
    for (int i=reg1Start; i<reg1Len+reg1Start; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    //println(mem.toHexString("Region1 cleared", reg1Start, reg1Len));

    //clear region 2
    mem.clear(reg2Start, reg2Len);
    for (int i=reg2Start; i<reg2Len+reg2Start; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    //println(mem.toHexString("Region2 cleared", reg2Start, reg2Len));

    //set all to ones
    byte b4 = 127;
    mem.fill(b4);
    for (int i=0; i<accessCapacity; i++) {
      assertEquals(mem.getByte(i), b4);
    }
    //println(mem.toHexString("Region1 + Region2 all ones", 0, accessCapacity));

    //clear all
    mem.clear();
    for (int i=0; i<accessCapacity; i++) {
      assertEquals(mem.getByte(i), zeroByte);
    }
    //println(mem.toHexString("Region1 + Region2 cleared", 0, accessCapacity));
  }

  @Test
  public void checkToHexStringAllMem() {
    int memCapacity = 48; //must be 48
    try (WritableHandle wrh1 = WritableMemory.allocateDirect(memCapacity)) {
      WritableMemory mem = wrh1.get();
      toHexStringAllMemTests(mem); //requires println enabled to visually check
    }
  }

  //enable println to visually check
  public static void toHexStringAllMemTests(WritableMemory mem) {
    int memCapacity = (int)mem.getCapacity();

    for (int i=0; i<memCapacity; i++) {
      mem.putByte(i, (byte)i);
    }

    //println(mem.toHexString("Check toHexString(0, 48) to integers", 0, memCapacity));
    //println(mem.toHexString("Check toHexString(8, 40)", 8, 40));
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
