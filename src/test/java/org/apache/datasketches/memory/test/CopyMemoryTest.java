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

import static org.apache.datasketches.memory.internal.Util.UNSAFE_COPY_THRESHOLD_BYTES;
import static org.testng.Assert.assertEquals;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.internal.MemoryImpl;
import org.apache.datasketches.memory.internal.WritableMemoryImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("javadoc")
public class CopyMemoryTest {

  @Test
  public void heapWSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    WritableMemoryImpl srcMem = genMem(k1, false); //!empty
    //println(srcMem.toHexString("src: ", 0, k1 << 3));
    WritableMemoryImpl dstMem = genMem(k2, true);
    srcMem.copyTo(0, dstMem, k1 << 3, k1 << 3);
    //println(dstMem.toHexString("dst: ", 0, k2 << 3));
    check(dstMem, k1, k1, 1);
  }

  @Test
  public void heapROSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    MemoryImpl srcMem = genMem(k1, false); //!empty
    WritableMemoryImpl dstMem = genMem(k2, true);
    srcMem.copyTo(0, dstMem, k1 << 3, k1 << 3);
    check(dstMem, k1, k1, 1);
  }

  @Test
  public void directWSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    try (WritableHandle wrh = genWRH(k1, false)) {
      WritableMemoryImpl srcMem = wrh.get();
      WritableMemoryImpl dstMem = genMem(k2, true);
      srcMem.copyTo(0, dstMem, k1 << 3, k1 << 3);
      check(dstMem, k1, k1, 1);
    }
  }

  @Test
  public void directROSource() {
    int k1 = 1 << 20; //longs
    int k2 = 2 * k1;
    try (WritableHandle wrh = genWRH(k1, false)) {
      MemoryImpl srcMem = wrh.get();
      WritableMemoryImpl dstMem = genMem(k2, true);
      srcMem.copyTo(0, dstMem, k1 << 3, k1 << 3);
      check(dstMem, k1, k1, 1);
    }
  }

  @Test
  public void heapWSrcRegion() {
    int k1 = 1 << 20; //longs
    //gen baseMem of k1 longs w data
    WritableMemoryImpl baseMem = genMem(k1, false); //!empty
    //gen src region of k1/2 longs, off= k1/2
    WritableMemoryImpl srcReg = baseMem.writableRegion((k1/2) << 3, (k1/2) << 3);
    WritableMemoryImpl dstMem = genMem(2 * k1, true); //empty
    srcReg.copyTo(0, dstMem, k1 << 3, (k1/2) << 3);
    //println(dstMem.toHexString("dstMem: ", k1 << 3, (k1/2) << 3));
    check(dstMem, k1, k1/2, (k1/2) + 1);
  }

  @Test
  public void heapROSrcRegion() {
    int k1 = 1 << 20; //longs
    //gen baseMem of k1 longs w data
    WritableMemoryImpl baseMem = genMem(k1, false); //!empty
    //gen src region of k1/2 longs, off= k1/2
    MemoryImpl srcReg = baseMem.region((k1/2) << 3, (k1/2) << 3);
    WritableMemoryImpl dstMem = genMem(2 * k1, true); //empty
    srcReg.copyTo(0, dstMem, k1 << 3, (k1/2) << 3);
    check(dstMem, k1, k1/2, (k1/2) + 1);
  }

  @Test
  public void directROSrcRegion() {
    int k1 = 1 << 20; //longs
    //gen baseMem of k1 longs w data, direct
    try (WritableHandle wrh = genWRH(k1, false)) {
      MemoryImpl baseMem = wrh.get();
      //gen src region of k1/2 longs, off= k1/2
      MemoryImpl srcReg = baseMem.region((k1/2) << 3, (k1/2) << 3);
      WritableMemoryImpl dstMem = genMem(2 * k1, true); //empty
      srcReg.copyTo(0, dstMem, k1 << 3, (k1/2) << 3);
      check(dstMem, k1, k1/2, (k1/2) + 1);
    }
  }

  @Test
  public void testOverlappingCopyLeftToRight() {
    byte[] bytes = new byte[((UNSAFE_COPY_THRESHOLD_BYTES * 5) / 2) + 1];
    ThreadLocalRandom.current().nextBytes(bytes);
    byte[] referenceBytes = bytes.clone();
    MemoryImpl referenceMem = MemoryImpl.wrap(referenceBytes);
    WritableMemoryImpl mem = WritableMemoryImpl.writableWrap(bytes);
    long copyLen = UNSAFE_COPY_THRESHOLD_BYTES * 2;
    mem.copyTo(0, mem, UNSAFE_COPY_THRESHOLD_BYTES / 2, copyLen);
    Assert.assertEquals(0, mem.compareTo(UNSAFE_COPY_THRESHOLD_BYTES / 2, copyLen, referenceMem, 0,
        copyLen));
  }

  @Test
  public void testOverlappingCopyRightToLeft() {
    byte[] bytes = new byte[((UNSAFE_COPY_THRESHOLD_BYTES * 5) / 2) + 1];
    ThreadLocalRandom.current().nextBytes(bytes);
    byte[] referenceBytes = bytes.clone();
    MemoryImpl referenceMem = MemoryImpl.wrap(referenceBytes);
    WritableMemoryImpl mem = WritableMemoryImpl.writableWrap(bytes);
    long copyLen = UNSAFE_COPY_THRESHOLD_BYTES * 2;
    mem.copyTo(UNSAFE_COPY_THRESHOLD_BYTES / 2, mem, 0, copyLen);
    Assert.assertEquals(0, mem.compareTo(0, copyLen, referenceMem, UNSAFE_COPY_THRESHOLD_BYTES / 2,
        copyLen));
  }

  private static void check(MemoryImpl mem, int offsetLongs, int lengthLongs, int startValue) {
    int offBytes = offsetLongs << 3;
    for (long i = 0; i < lengthLongs; i++) {
      assertEquals(mem.getLong(offBytes + (i << 3)), i + startValue);
    }
  }

  private static WritableHandle genWRH(int longs, boolean empty) {
    WritableHandle wrh = WritableMemoryImpl.allocateDirect(longs << 3);
    WritableMemoryImpl mem = wrh.get();
    if (empty) {
      mem.clear();
    } else {
      for (int i = 0; i < longs; i++) { mem.putLong(i << 3, i + 1); }
    }
    return wrh;
  }


  private static WritableMemoryImpl genMem(int longs, boolean empty) {
    WritableMemoryImpl mem = WritableMemoryImpl.allocate(longs << 3);
    if (!empty) {
      for (int i = 0; i < longs; i++) { mem.putLong(i << 3, i + 1); }
    }
    return mem;
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
