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

/*
 * Note: Lincoln's Gettysburg Address is in the public domain. See LICENSE.
 */

package org.apache.datasketches.memory.internal;

import static org.apache.datasketches.memory.internal.BaseStateImpl.LS;
import static org.apache.datasketches.memory.internal.BaseStateImpl.NATIVE_BYTE_ORDER;
import static org.apache.datasketches.memory.internal.Util.getResourceFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;

import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
public class AllocateDirectMapMemoryTest {
  MapHandle hand = null;

  @BeforeClass
  public void setReadOnly() {
    UtilTest.setGettysburgAddressFileToReadOnly();
  }

  @Test
  public void simpleMap() throws Exception {
    File file = getResourceFile("GettysburgAddress.txt");
    assertTrue(AllocateDirectMap.isFileReadOnly(file));
    try (MapHandle rh = Memory.map(file)) {
      rh.close();
    }
  }

  @Test
  public void testIllegalArguments() throws Exception {
    File file = getResourceFile("GettysburgAddress.txt");
    try (MapHandle rh = Memory.map(file, -1, Integer.MAX_VALUE, NATIVE_BYTE_ORDER)) {
      fail("Failed: testIllegalArgumentException: Position was negative.");
    } catch (IllegalArgumentException e) {
      //ok
    }

    try (MapHandle rh = Memory.map(file, 0, -1, NATIVE_BYTE_ORDER)) {
      fail("Failed: testIllegalArgumentException: Size was negative.");
    } catch (IllegalArgumentException e) {
      //ok
    }
  }

  @Test
  public void testMapAndMultipleClose() throws IllegalStateException {
    File file = getResourceFile("GettysburgAddress.txt");
    long memCapacity = file.length();
    try (MapHandle rh = Memory.map(file, 0, memCapacity, NATIVE_BYTE_ORDER)) {
      Memory map = rh.get();
      assertEquals(memCapacity, map.getCapacity());
      rh.close();
      rh.close();
      map.getCapacity();
    } catch (Exception e) {
      //OK
    }
  }

  @Test
  public void testReadFailAfterClose() throws IllegalStateException {
    File file = getResourceFile("GettysburgAddress.txt");
    long memCapacity = file.length();
    try (MapHandle rh = Memory.map(file, 0, memCapacity, NATIVE_BYTE_ORDER)) {
      Memory mmf = rh.get();
      rh.close();
      mmf.getByte(0);
    } catch (Exception e) {
      //OK
    }
  }

  @Test
  public void testLoad() throws Exception {
    File file = getResourceFile("GettysburgAddress.txt");
    long memCapacity = file.length();
    try (MapHandle rh = Memory.map(file, 0, memCapacity, NATIVE_BYTE_ORDER)) {
      rh.load();
      assertTrue(rh.isLoaded());
      rh.close();
    }
  }

  @Test
  public void testHandlerHandoffWithTWR() throws Exception {
    File file = getResourceFile("GettysburgAddress.txt");
    long memCapacity = file.length();
    Memory mem;
    try (MapHandle rh = Memory.map(file, 0, memCapacity, NATIVE_BYTE_ORDER)) {
      rh.load();
      assertTrue(rh.isLoaded());
      hand = rh;
      mem = rh.get();
    } //TWR closes
    assertFalse(mem.isValid());
    //println(""+mem.isValid());
  }

  @Test
  public void testHandoffWithoutClose() throws Exception {
    File file = getResourceFile("GettysburgAddress.txt");
    long memCapacity = file.length();
    MapHandle rh = Memory.map(file, 0, memCapacity, NATIVE_BYTE_ORDER);
    rh.load();
    assertTrue(rh.isLoaded());
    hand = rh;
    //The receiver of the handler must close the resource, in this case it is the class.
  }

  @AfterClass
  public void afterAllTests() throws Exception {
      if (hand != null) {
      Memory mem = hand.get();
      if ((mem != null) && mem.isValid()) {
        hand.close();
        assertFalse(mem.isValid());
      }
    }
  }

  @Test
  public void printlnTest() {
    println("PRINTING: "+this.getClass().getName());
  }

  static void println(final Object o) {
    if (o == null) { print(LS); }
    else { print(o.toString() + LS); }
  }

  /**
   * @param o value to print
   */
  static void print(final Object o) {
    if (o != null) {
      //System.out.print(o.toString()); //disable here
    }
  }

}
