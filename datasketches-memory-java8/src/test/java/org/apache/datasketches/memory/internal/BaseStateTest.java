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

import static org.apache.datasketches.memory.internal.BaseStateImpl.NATIVE_BYTE_ORDER;
import static org.apache.datasketches.memory.internal.BaseStateImpl.checkJavaVersion;
import static org.apache.datasketches.memory.internal.BaseStateImpl.isNativeByteOrder;
import static org.apache.datasketches.memory.internal.BaseStateImpl.parseJavaVersion;
import static org.apache.datasketches.memory.internal.BaseStateImpl.typeDecode;
import static org.apache.datasketches.memory.internal.UnsafeUtil.ARRAY_DOUBLE_INDEX_SCALE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class BaseStateTest {

  @Test
  public void checkJdkString() {
    String jdkVer;
    int[] p = new int[2];
    String[] good1_Strings = {"1.8.0_121", "8", "11"};
    int len = good1_Strings.length;
    for (int i = 0; i < len; i++) {
      jdkVer = good1_Strings[i];
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]);
      int jdkMajor = (p[0] == 1) ? p[1] : p[0]; //model the actual JDK_MAJOR
      if (p[0] == 1) { assertTrue(jdkMajor == p[1]); }
      if (p[0] > 1 ) { assertTrue(jdkMajor == p[0]); }
    }
    try {
      jdkVer = "14.0.4"; //ver 14 string
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]);
      fail();
    } catch (IllegalArgumentException e) {
      println("" + e);
    }

    try {
      jdkVer = "1.7.0_80"; //1.7 string
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]);
      fail();
    } catch (IllegalArgumentException e) {
      println("" + e);
    }
    try {
      jdkVer = "1.6.0_65"; //valid string but < 1.7
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]); //throws
      fail();
    } catch (IllegalArgumentException e) {
      println("" + e);
    }
    try {
      jdkVer = "b"; //invalid string
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]); //throws
      fail();
    } catch (IllegalArgumentException e) {
      println("" + e);
    }
    try {
      jdkVer = ""; //invalid string
      p = parseJavaVersion(jdkVer);
      checkJavaVersion(jdkVer, p[0], p[1]); //throws
      fail();
    } catch (IllegalArgumentException e) {
      println("" + e);
    }
  }

  @Test
  public void checkPrimOffset() {
    int off = (int)Prim.BYTE.off();
    assertTrue(off > 0);
  }

  @Test
  public void checkIsSameResource() {
    WritableMemory wmem = WritableMemory.allocate(16);
    Memory mem = wmem;
    assertFalse(wmem.isSameResource(null));
    assertTrue(wmem.isSameResource(mem));

    WritableBuffer wbuf = wmem.asWritableBuffer();
    Buffer buf = wbuf;
    assertFalse(wbuf.isSameResource(null));
    assertTrue(wbuf.isSameResource(buf));
  }

  //StepBoolean checks
  @Test
  public void checkStepBoolean() {
    checkStepBoolean(true);
    checkStepBoolean(false);
  }

  private static void checkStepBoolean(boolean initialState) {
    StepBoolean step = new StepBoolean(initialState);
    assertTrue(step.get() == initialState); //confirm initialState
    step.change();
    assertTrue(step.hasChanged());      //1st change was successful
    assertTrue(step.get() != initialState); //confirm it is different from initialState
    step.change();
    assertTrue(step.get() != initialState); //Still different from initialState
    assertTrue(step.hasChanged());  //confirm it was changed from initialState value
  }

  @Test
  public void checkPrim() {
    assertEquals(Prim.DOUBLE.scale(), ARRAY_DOUBLE_INDEX_SCALE);
  }

  @Test
  public void checkGetNativeBaseOffset_Heap() throws Exception {
    WritableMemory wmem = WritableMemory.allocate(8); //heap
    final long offset = ((BaseStateImpl)wmem).getNativeBaseOffset();
    assertEquals(offset, 0L);
  }

  @Test
  public void checkIsByteOrderCompatible() {
    WritableMemory wmem = WritableMemory.allocate(8);
    assertTrue(wmem.isByteOrderCompatible(NATIVE_BYTE_ORDER));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkByteOrderNull() {
    Util.isNativeByteOrder(null);
    fail();
  }

  @Test
  public void checkIsNativeByteOrder() {
    assertTrue(isNativeByteOrder(NATIVE_BYTE_ORDER));
    try {
      isNativeByteOrder(null);
      fail();
    } catch (final IllegalArgumentException e) {}
  }

  @Test
  public void checkXxHash64() {
    WritableMemory mem = WritableMemory.allocate(8);
    long out = mem.xxHash64(mem.getLong(0), 1L);
    assertTrue(out != 0);
  }

  @Test
  public void checkTypeDecode() {
    for (int i = 0; i < 256; i++) {
      String str = typeDecode(i);
      println(i + "\t" + str);
    }
  }

  /********************/
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
