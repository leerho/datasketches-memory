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

import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.internal.MemoryImpl;
import org.apache.datasketches.memory.internal.WritableMemoryImpl;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class CopyMemoryOverlapTest {

  @Test
  public void checkOverlapUsingMemory() {
    long copyLongs = 1 << 20;
    double overlap = 0.5;
    long start_mS = System.currentTimeMillis();

    copyUsingDirectMemory(copyLongs, overlap, true);
    long end1_mS = System.currentTimeMillis();

    copyUsingDirectMemory(copyLongs, overlap, false);
    long end2_mS = System.currentTimeMillis();

    println("CopyUp Time Sec: " + ((end1_mS - start_mS)/1000.0));
    println("CopyDn Time Sec: " + ((end2_mS - end1_mS)/1000.0));
  }

  @Test
  public void checkOverlapUsingRegions() {
    long copyLongs = 1 << 20;
    double overlap = 0.5;
    long start_mS = System.currentTimeMillis();

    copyUsingDirectRegions(copyLongs, overlap, true);
    long end1_mS = System.currentTimeMillis();

    copyUsingDirectRegions(copyLongs, overlap, false);
    long end2_mS = System.currentTimeMillis();

    println("CopyUp Time Sec: " + ((end1_mS - start_mS)/1000.0));
    println("CopyDn Time Sec: " + ((end2_mS - end1_mS)/1000.0));
  }

  private static final void copyUsingDirectMemory(long copyLongs, double overlap, boolean copyUp) {
    println("Copy Using Direct MemoryImpl");
    long overlapLongs = (long) (overlap * copyLongs);
    long backingLongs = (2 * copyLongs) - overlapLongs;

    long fromOffsetLongs;
    long toOffsetLongs;
    //long deltaLongs;

    if (copyUp) {
      fromOffsetLongs = 0;
      toOffsetLongs = copyLongs - overlapLongs;
      //deltaLongs = toOffsetLongs - fromOffsetLongs;
    } else {
      fromOffsetLongs = copyLongs - overlapLongs;
      toOffsetLongs = 0;
      //deltaLongs = toOffsetLongs - fromOffsetLongs;
    }

    long backingBytes = backingLongs << 3;
    long copyBytes = copyLongs << 3;
    long fromOffsetBytes = fromOffsetLongs << 3;
    long toOffsetBytes = toOffsetLongs << 3;
    //long deltaBytes = deltaLongs << 3;
    println("Copy longs   : " + copyLongs    + "\t bytes: " + copyBytes);
    println("Overlap      : " + (overlap * 100.0) + "%");
    println("CopyUp       : " + copyUp);
    println("Backing longs: " + backingLongs + "\t bytes: " + backingBytes);

    try (WritableHandle backHandle = WritableMemoryImpl.allocateDirect(backingBytes)) {
      WritableMemoryImpl backingMem = backHandle.get();
      fill(backingMem); //fill mem with 0 thru copyLongs -1
      //listMem(backingMem, "Original");
      backingMem.copyTo(fromOffsetBytes, backingMem, toOffsetBytes, copyBytes);
      //listMem(backingMem, "After");
      checkMemLongs(backingMem, fromOffsetLongs, toOffsetLongs, copyLongs);
    }
    println("");
  }

  private static final void copyUsingDirectRegions(long copyLongs, double overlap, boolean copyUp) {
    println("Copy Using Direct MemoryImpl");
    long overlapLongs = (long) (overlap * copyLongs);
    long backingLongs = (2 * copyLongs) - overlapLongs;

    long fromOffsetLongs;
    long toOffsetLongs;
    //long deltaLongs;

    if (copyUp) {
      fromOffsetLongs = 0;
      toOffsetLongs = copyLongs - overlapLongs;
      //deltaLongs = toOffsetLongs - fromOffsetLongs;
    } else {
      fromOffsetLongs = copyLongs - overlapLongs;
      toOffsetLongs = 0;
      //deltaLongs = toOffsetLongs - fromOffsetLongs;
    }

    long backingBytes = backingLongs << 3;
    long copyBytes = copyLongs << 3;
    long fromOffsetBytes = fromOffsetLongs << 3;
    long toOffsetBytes = toOffsetLongs << 3;
    //long deltaBytes = deltaLongs << 3;
    println("Copy longs   : " + copyLongs    + "\t bytes: " + copyBytes);
    println("Overlap      : " + (overlap * 100.0) + "%");
    println("CopyUp       : " + copyUp);
    println("Backing longs: " + backingLongs + "\t bytes: " + backingBytes);

    try (WritableHandle backHandle = WritableMemoryImpl.allocateDirect(backingBytes)) {
      WritableMemoryImpl backingMem = backHandle.get();
      fill(backingMem); //fill mem with 0 thru copyLongs -1
      //listMem(backingMem, "Original");
      WritableMemoryImpl reg1 = backingMem.writableRegion(fromOffsetBytes, copyBytes);
      WritableMemoryImpl reg2 = backingMem.writableRegion(toOffsetBytes, copyBytes);

      reg1.copyTo(0, reg2, 0, copyBytes);
      //listMem(backingMem, "After");
      checkMemLongs(reg2, fromOffsetLongs, 0, copyLongs);
    }
    println("");
  }

  private static final void fill(WritableMemoryImpl wmem) {
    long longs = wmem.getCapacity() >>> 3;
    for (long i = 0; i < longs; i++) { wmem.putLong(i << 3, i); } //fill with 0 .. (longs - 1)
    //checkMemLongs(wmem, 0L, 0L, longs);
  }

  private static final void checkMemLongs(MemoryImpl mem, long fromOffsetLongs, long toOffsetLongs, long copyLongs) {
    for (long i = 0; i < copyLongs; i++) {
      long memVal = mem.getLong((toOffsetLongs + i) << 3);
      assertEquals(memVal, fromOffsetLongs + i);
    }
  }

  @SuppressWarnings("unused")
  private static final void listMem(MemoryImpl mem, String comment) {
    println(comment);
    println("Idx\tValue");
    long longs = mem.getCapacity() >>> 3;
    for (long i = 0; i < longs; i++) {
      println(i + "\t" + mem.getLong(i << 3));
    }
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
