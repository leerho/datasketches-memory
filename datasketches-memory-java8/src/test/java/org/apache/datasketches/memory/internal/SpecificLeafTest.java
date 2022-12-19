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

import static org.apache.datasketches.memory.internal.ResourceImpl.NATIVE_BYTE_ORDER;
import static org.apache.datasketches.memory.internal.ResourceImpl.NON_NATIVE_BYTE_ORDER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMapHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class SpecificLeafTest {

  @Test
  public void checkByteBufferLeafs() {
    int bytes = 128;
    ByteBuffer bb = ByteBuffer.allocate(bytes);
    bb.order(NATIVE_BYTE_ORDER);

    Memory mem = Memory.wrap(bb).region(0, bytes, NATIVE_BYTE_ORDER);
    assertTrue(((ResourceImpl)mem).isByteBufferResource());
    assertTrue(mem.isReadOnly());
    assertTrue(((ResourceImpl)mem).isMemoryApi());
    assertFalse(((ResourceImpl)mem).isDirectResource());
    assertFalse(((ResourceImpl)mem).isMemoryMappedFileResource());
    checkCrossLeafTypeIds(mem);
    Buffer buf = mem.asBuffer().region(0, bytes, NATIVE_BYTE_ORDER);
    assertEquals(buf.getByteOrder(), NATIVE_BYTE_ORDER);

    bb.order(NON_NATIVE_BYTE_ORDER);
    Memory mem2 = Memory.wrap(bb).region(0, bytes, NON_NATIVE_BYTE_ORDER);
    Buffer buf2 = mem2.asBuffer().region(0, bytes, NON_NATIVE_BYTE_ORDER);
    Buffer buf3 = buf2.duplicate();

    assertTrue(((ResourceImpl)mem).isRegionView());
    assertTrue(((ResourceImpl)mem2).isRegionView());
    assertTrue(((ResourceImpl)buf).isRegionView());
    assertTrue(((ResourceImpl)buf2).isRegionView());
    assertTrue(((ResourceImpl)buf3).isDuplicateBufferView());
  }

  @Test
  public void checkDirectLeafs() throws Exception {
    int bytes = 128;
    try (WritableHandle h = WritableMemory.allocateDirect(bytes)) {
      WritableMemory wmem = h.getWritable(); //native mem
      assertTrue(((ResourceImpl)wmem).isDirectResource());
      assertFalse(wmem.isReadOnly());
      checkCrossLeafTypeIds(wmem);
      WritableMemory nnwmem = wmem.writableRegion(0, bytes, NON_NATIVE_BYTE_ORDER);

      Memory mem = wmem.region(0, bytes, NATIVE_BYTE_ORDER);
      Buffer buf = mem.asBuffer().region(0, bytes, NATIVE_BYTE_ORDER);


      Memory mem2 = nnwmem.region(0, bytes, NON_NATIVE_BYTE_ORDER);
      Buffer buf2 = mem2.asBuffer().region(0, bytes, NON_NATIVE_BYTE_ORDER);
      Buffer buf3 = buf2.duplicate();

      assertTrue(((ResourceImpl)mem).isRegionView());
      assertTrue(((ResourceImpl)mem2).isRegionView());
      assertTrue(((ResourceImpl)buf).isRegionView());
      assertTrue(((ResourceImpl)buf2).isRegionView());
      assertTrue(((ResourceImpl)buf3).isDuplicateBufferView());
      assertTrue(((ResourceImpl)mem).isMemoryApi());
    }
  }

  @Test
  public void checkMapLeafs() throws Exception {
    File file = new File("TestFile2.bin");
    if (file.exists()) {
      try {
        java.nio.file.Files.delete(file.toPath());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    assertTrue(file.createNewFile());
    assertTrue(file.setWritable(true, false)); //writable=true, ownerOnly=false
    assertTrue(file.isFile());
    file.deleteOnExit();  //comment out if you want to examine the file.

    final long bytes = 128;

    try (WritableMapHandle h = WritableMemory.writableMap(file, 0L, bytes, NATIVE_BYTE_ORDER)) {
      WritableMemory mem = h.getWritable(); //native mem
      assertTrue(((ResourceImpl)mem).isMemoryMappedFileResource());
      assertFalse(mem.isReadOnly());
      checkCrossLeafTypeIds(mem);
      Memory nnreg = mem.region(0, bytes, NON_NATIVE_BYTE_ORDER);

      Memory reg = mem.region(0, bytes, NATIVE_BYTE_ORDER);
      Buffer buf = reg.asBuffer().region(0, bytes, NATIVE_BYTE_ORDER);
      Buffer buf4 = buf.duplicate();

      Memory reg2 = nnreg.region(0, bytes, NON_NATIVE_BYTE_ORDER);
      Buffer buf2 = reg2.asBuffer().region(0, bytes, NON_NATIVE_BYTE_ORDER);
      Buffer buf3 = buf2.duplicate();

      assertTrue(((ResourceImpl)reg).isRegionView());
      assertTrue(((ResourceImpl)reg2).isRegionView());
      assertTrue(((ResourceImpl)buf).isRegionView());
      assertTrue(((ResourceImpl)buf2).isRegionView());
      assertTrue(((ResourceImpl)buf3).isDuplicateBufferView());
      assertTrue(((ResourceImpl)buf4).isDuplicateBufferView());
    }
  }

  @Test
  public void checkHeapLeafs() {
    int bytes = 128;
    Memory mem = Memory.wrap(new byte[bytes]);
    assertFalse(((ResourceImpl)mem).isDirectResource());
    assertTrue(((ResourceImpl)mem).isReadOnly());
    checkCrossLeafTypeIds(mem);
    Memory nnreg = mem.region(0, bytes, NON_NATIVE_BYTE_ORDER);

    Memory reg = mem.region(0, bytes, NATIVE_BYTE_ORDER);
    Buffer buf = reg.asBuffer().region(0, bytes, NATIVE_BYTE_ORDER);
    Buffer buf4 = buf.duplicate();

    Memory reg2 = nnreg.region(0, bytes, NON_NATIVE_BYTE_ORDER);
    Buffer buf2 = reg2.asBuffer().region(0, bytes, NON_NATIVE_BYTE_ORDER);
    Buffer buf3 = buf2.duplicate();

    assertFalse(((ResourceImpl)mem).isRegionView());
    assertTrue(((ResourceImpl)reg2).isRegionView());
    assertTrue(((ResourceImpl)buf).isRegionView());
    assertTrue(((ResourceImpl)buf2).isRegionView());
    assertTrue(((ResourceImpl)buf3).isDuplicateBufferView());
    assertTrue(((ResourceImpl)buf4).isDuplicateBufferView());
  }

  private static void checkCrossLeafTypeIds(Memory mem) {
    Memory reg1 = mem.region(0, mem.getCapacity());
    assertTrue(((ResourceImpl)reg1).isRegionView());

    Buffer buf1 = reg1.asBuffer();
    assertTrue(((ResourceImpl)buf1).isRegionView());
    assertFalse(((ResourceImpl)buf1).isMemoryApi());
    assertTrue(buf1.isReadOnly());

    Buffer buf2 = buf1.duplicate();
    assertTrue(((ResourceImpl)buf2).isRegionView());
    assertFalse(((ResourceImpl)buf1).isMemoryApi());
    assertTrue(((ResourceImpl)buf2).isDuplicateBufferView());
    assertTrue(buf2.isReadOnly());

    Memory mem2 = buf1.asMemory(); //
    assertTrue(((ResourceImpl)mem2).isRegionView());
    assertFalse(((ResourceImpl)buf1).isMemoryApi());
    assertFalse(((ResourceImpl)mem2).isDuplicateBufferView());
    assertTrue(mem2.isReadOnly());

    Buffer buf3 = buf1.duplicate(NON_NATIVE_BYTE_ORDER);
    assertTrue(((ResourceImpl)buf3).isRegionView());
    assertFalse(((ResourceImpl)buf1).isMemoryApi());
    assertTrue(((ResourceImpl)buf3).isDuplicateBufferView());
    assertTrue(((ResourceImpl)buf3).isNonNativeOrder());
    assertTrue(buf3.isReadOnly());

    Memory mem3 = buf3.asMemory();
    assertTrue(((ResourceImpl)mem3).isRegionView());
    assertTrue(((ResourceImpl)mem3).isMemoryApi());
    assertTrue(((ResourceImpl)mem3).isDuplicateBufferView());
    assertTrue(((ResourceImpl)mem3).isNonNativeOrder());
    assertTrue(mem3.isReadOnly());
  }

}
