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
import static org.apache.datasketches.memory.internal.BaseStateImpl.NON_NATIVE_BYTE_ORDER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableBuffer;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMapHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class LeafImplTest {
  private static final MemoryRequestServer dummyMemReqSvr = new DummyMemoryRequestServer();

  private static ByteOrder otherByteOrder(final ByteOrder order) {
    return (order == NATIVE_BYTE_ORDER) ? NON_NATIVE_BYTE_ORDER : NATIVE_BYTE_ORDER;
  }

  static class DummyMemoryRequestServer implements MemoryRequestServer {
    @Override
    public WritableMemory request(WritableMemory currentWMem, long capacityBytes) { return null; }
    @Override
    public void requestClose(WritableMemory memToClose, WritableMemory newMemory) { }
  }

  @Test
  public void checkDirectLeafs() throws Exception {
    long off = 0;
    long cap = 128;
    // Off Heap, Native order, No ByteBuffer, has MemReqSvr
    try (WritableHandle wdh = WritableMemory.allocateDirect(cap, NATIVE_BYTE_ORDER, dummyMemReqSvr)) {
      WritableMemory memNO = wdh.getWritable();
      memNO.putShort(0, (short) 1);
      assertNull(((BaseStateImpl)memNO).getUnsafeObject());
      assertTrue(memNO.isDirectResource());
      checkCombinations(memNO, off, cap, memNO.isDirectResource(), NATIVE_BYTE_ORDER, false, true);
    }
    // Off Heap, Non Native order, No ByteBuffer, has MemReqSvr
    try (WritableHandle wdh = WritableMemory.allocateDirect(cap, NON_NATIVE_BYTE_ORDER, dummyMemReqSvr)) {
      WritableMemory memNNO = wdh.getWritable();
      memNNO.putShort(0, (short) 1);
      assertNull(((BaseStateImpl)memNNO).getUnsafeObject());
      assertTrue(memNNO.isDirectResource());
      checkCombinations(memNNO, off, cap, memNNO.isDirectResource(), NON_NATIVE_BYTE_ORDER, false, true);
    }
  }

  @Test
  public void checkByteBufferLeafs() {
    long off = 0;
    long cap = 128;
    //BB on heap, native order, has ByteBuffer, has MemReqSvr
    ByteBuffer bb = ByteBuffer.allocate((int)cap);
    bb.order(NATIVE_BYTE_ORDER);
    bb.putShort(0, (short) 1);
    WritableMemory mem = WritableMemory.writableWrap(bb, NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(bb.isDirect(), mem.isDirectResource());
    assertNotNull(((BaseStateImpl)mem).getUnsafeObject());
    checkCombinations(mem, off, cap, mem.isDirectResource(), mem.getByteOrder(), true, true);

    //BB off heap, native order, has ByteBuffer, has MemReqSvr
    ByteBuffer dbb = ByteBuffer.allocateDirect((int)cap);
    dbb.order(NATIVE_BYTE_ORDER);
    dbb.putShort(0, (short) 1);
    mem = WritableMemory.writableWrap(dbb, NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(dbb.isDirect(), mem.isDirectResource());
    assertNull(((BaseStateImpl)mem).getUnsafeObject());
    checkCombinations(mem, off, cap,  mem.isDirectResource(), mem.getByteOrder(), true, true);

    //BB on heap, non native order, has ByteBuffer, has MemReqSvr
    bb = ByteBuffer.allocate((int)cap);
    bb.order(NON_NATIVE_BYTE_ORDER);
    bb.putShort(0, (short) 1);
    mem = WritableMemory.writableWrap(bb, NON_NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(bb.isDirect(), mem.isDirectResource());
    assertNotNull(((BaseStateImpl)mem).getUnsafeObject());
    checkCombinations(mem, off, cap, mem.isDirectResource(), mem.getByteOrder(), true, true);

    //BB off heap, non native order, has ByteBuffer, has MemReqSvr
    dbb = ByteBuffer.allocateDirect((int)cap);
    dbb.order(NON_NATIVE_BYTE_ORDER);
    dbb.putShort(0, (short) 1);
    mem = WritableMemory.writableWrap(dbb, NON_NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(dbb.isDirect(), mem.isDirectResource());
    assertNull(((BaseStateImpl)mem).getUnsafeObject());
    checkCombinations(mem, off, cap,  mem.isDirectResource(), mem.getByteOrder(), true, true);
  }

  @Test
  public void checkMapLeafs() throws Exception {
    long off = 0;
    long cap = 128;
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
    // Off Heap, Native order, No ByteBuffer, No MemReqSvr
    try (WritableMapHandle wmh = WritableMemory.writableMap(file, off, cap, NATIVE_BYTE_ORDER)) {
      WritableMemory memNO = wmh.getWritable();
      memNO.putShort(0, (short) 1);
      assertNull(((BaseStateImpl)memNO).getUnsafeObject());
      assertTrue(memNO.isDirectResource());
      checkCombinations(memNO, off, cap, memNO.isDirectResource(), NATIVE_BYTE_ORDER, false, false);
    }
    // Off heap, Non Native order, No ByteBuffer, no MemReqSvr
    try (WritableMapHandle wmh = WritableMemory.writableMap(file, off, cap, NON_NATIVE_BYTE_ORDER)) {
      WritableMemory memNNO = wmh.getWritable();
      memNNO.putShort(0, (short) 1);
      assertNull(((BaseStateImpl)memNNO).getUnsafeObject());
      assertTrue(memNNO.isDirectResource());
      checkCombinations(memNNO, off, cap, memNNO.isDirectResource(), NON_NATIVE_BYTE_ORDER, false, false);
    }
  }

  @Test
  public void checkHeapLeafs() {
    long off = 0;
    long cap = 128;
    // On Heap, Native order, No ByteBuffer, No MemReqSvr
    WritableMemory memNO = WritableMemory.allocate((int)cap); //assumes NATIVE_BYTE_ORDER
    memNO.putShort(0, (short) 1);
    assertNotNull(((BaseStateImpl)memNO).getUnsafeObject());
    assertFalse(memNO.isDirectResource());
    checkCombinations(memNO, off, cap, memNO.isDirectResource(), NATIVE_BYTE_ORDER, false, false);
    // On Heap, Non-native order, No ByteBuffer, No MemReqSvr
    WritableMemory memNNO = WritableMemory.allocate((int)cap, NON_NATIVE_BYTE_ORDER);
    memNNO.putShort(0, (short) 1);
    assertNotNull(((BaseStateImpl)memNNO).getUnsafeObject());
    assertFalse(memNNO.isDirectResource());
    checkCombinations(memNNO, off, cap, memNNO.isDirectResource(), NON_NATIVE_BYTE_ORDER, false, false);
  }

  private static void checkCombinations(WritableMemory mem, long off, long cap,
      boolean direct, ByteOrder bo, boolean hasByteBuffer, boolean hasMemReqSvr) {
    ByteOrder oo = otherByteOrder(bo);

    assertEquals(mem.writableRegion(off, cap, bo).getShort(0), 1);
    assertEquals(mem.writableRegion(off, cap, oo).getShort(0), 256);

    assertEquals(mem.asWritableBuffer(bo).getShort(0), 1);
    assertEquals(mem.asWritableBuffer(oo).getShort(0), 256);

    ByteBuffer bb = ((BaseStateImpl)mem).getByteBuffer();
    assertTrue( hasByteBuffer ? bb != null : bb == null);

    assertTrue(mem.getByteOrder() == bo);

    if (hasMemReqSvr) { assertTrue(mem.getMemoryRequestServer() instanceof DummyMemoryRequestServer); }

    Object obj = ((BaseStateImpl)mem).getUnsafeObject();
    if (direct) {
      assertTrue(mem.isDirectResource());
      assertNull(obj);
    } else {
      assertFalse(mem.isDirectResource());
      assertNotNull(obj);
    }

    assertTrue(mem.isAlive() == true);

    WritableBuffer buf = mem.asWritableBuffer();

    assertEquals(buf.writableRegion(off, cap, bo).getShort(0), 1);
    assertEquals(buf.writableRegion(off, cap, oo).getShort(0), 256);
    assertEquals(buf.writableDuplicate(bo).getShort(0), 1);
    assertEquals(buf.writableDuplicate(oo).getShort(0), 256);

    bb = ((BaseStateImpl)buf).getByteBuffer();
    assertTrue(hasByteBuffer ? bb != null : bb == null);

    assertTrue(buf.getByteOrder() == bo);

    if (hasMemReqSvr) { assertTrue(buf.getMemoryRequestServer() instanceof DummyMemoryRequestServer); }

    obj = ((BaseStateImpl)buf).getUnsafeObject();
    if (direct) {
      assertTrue(buf.isDirectResource());
      assertNull(obj);
    } else {
      assertFalse(buf.isDirectResource());
      assertNotNull(obj);
    }

    assertTrue(buf.isAlive() == true);

    WritableMemory nnMem = mem.writableRegion(off, cap, oo);

    assertEquals(nnMem.writableRegion(off, cap, bo).getShort(0), 1);
    assertEquals(nnMem.writableRegion(off, cap, oo).getShort(0), 256);
    assertEquals(nnMem.asWritableBuffer(bo).getShort(0), 1);
    assertEquals(nnMem.asWritableBuffer(oo).getShort(0), 256);

    bb = ((BaseStateImpl)nnMem).getByteBuffer();
    assertTrue( hasByteBuffer ? bb != null : bb == null);

    assertTrue(nnMem.getByteOrder() == oo);

    if (hasMemReqSvr) { assertTrue(nnMem.getMemoryRequestServer() instanceof DummyMemoryRequestServer); }

    obj = ((BaseStateImpl)nnMem).getUnsafeObject();
    if (direct) {
      assertTrue(nnMem.isDirectResource());
      assertNull(obj);
    } else {
      assertFalse(nnMem.isDirectResource());
      assertNotNull(obj);
    }

    assertTrue(nnMem.isAlive() == true);

    WritableBuffer nnBuf = mem.asWritableBuffer(oo);

    assertEquals(nnBuf.writableRegion(off, cap, bo).getShort(0), 1);
    assertEquals(nnBuf.writableRegion(off, cap, oo).getShort(0), 256);
    assertEquals(nnBuf.writableDuplicate(bo).getShort(0), 1);
    assertEquals(nnBuf.writableDuplicate(oo).getShort(0), 256);

    bb = ((BaseStateImpl)nnBuf).getByteBuffer();
    assertTrue( hasByteBuffer ? bb != null : bb == null);

    assertTrue(nnBuf.getByteOrder() == oo);

    if (hasMemReqSvr) { assertTrue(nnBuf.getMemoryRequestServer() instanceof DummyMemoryRequestServer); }

    obj = ((BaseStateImpl)nnBuf).getUnsafeObject();
    if (direct) {
      assertTrue(nnBuf.isDirectResource());
      assertNull(obj);
    } else {
      assertFalse(nnBuf.isDirectResource());
      assertNotNull(obj);
    }

    assertTrue(nnBuf.isAlive() == true);
  }

  @Test
  public void confirmByteBufferView() {
    long le = 0X0807060504030201L;
    long be = 0X0102030405060708L;
    ByteBuffer bb = ByteBuffer.allocate(128);
    bb.order(NATIVE_BYTE_ORDER);
    bb.putLong(0, le);
    WritableMemory mem = WritableMemory.writableWrap(bb, NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(le,  mem.getLong(0)); //mem reads what BB put
    mem.putLong(0, be);
    assertEquals(be, bb.getLong(0)); //BB reads what mem put
    //This confirms that mem and BB both have a R/W view of the same backing store

    //Changing the byte order never changes what is already in the backing store
    //It will change how bytes in the store are interpreted going in or coming out.
    bb.order(NON_NATIVE_BYTE_ORDER);
    assertEquals(le, bb.getLong(0)); //converts to LE on read;
    assertEquals(be, mem.getLong(0)); //backing memory is still BE!

    WritableMemory mem2 = WritableMemory.writableWrap(bb, NON_NATIVE_BYTE_ORDER, dummyMemReqSvr);
    assertEquals(le, mem2.getLong(0)); //converts to LE on read;
    mem2.putLong(0, le); //should put it as BE
    assertEquals(le, mem2.getLong(0)); //converts to LE on read;
  }

}
