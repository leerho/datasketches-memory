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
package org.apache.datasketches.memory;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

/**
 * 
 */
public class WritableMemoryIT {

    @Test
    public void testAllocateInt() {
        WritableMemory mem = WritableMemory.allocate(100);
        assertNotNull(mem);
    }
    
    @Test
    public void testWritableWrap() {
        ByteBuffer bb = ByteBuffer.allocate(26);
        
        WritableMemory mem = WritableMemory.writableWrap(bb);
        
        for (long ii = 0; ii < 26; ii++) {
            mem.putByte(ii, (byte) ('a' + ii));
        }
        
        for (int ii = 0; ii < 26; ++ii) {
            assertEquals(bb.get(ii), 'a'+ii);
        }
    }
    
    @Test
    public void testWritableWrapDirect() {
        ByteBuffer bb = ByteBuffer.allocateDirect(26);
        
        WritableMemory mem = WritableMemory.writableWrap(bb);
        
        for (long ii = 0; ii < 26; ii++) {
            mem.putByte(ii, (byte) ('a' + ii));
        }
        
        for (int ii = 0; ii < 26; ++ii) {
            assertEquals(bb.get(ii), 'a'+ii);
        }
    }
    
    @Test
    public void testWritableWrapEndian() {
        ByteBuffer bb = ByteBuffer.allocateDirect(4*Integer.BYTES);
        ByteBuffer bl = ByteBuffer.allocate(4*Integer.BYTES);
        
        WritableMemory bigEndianMem = WritableMemory.writableWrap(bb, ByteOrder.BIG_ENDIAN);
        WritableMemory littleEndianMem = WritableMemory.writableWrap(bl, ByteOrder.LITTLE_ENDIAN);
        
        for (int ii = 0; ii < 4; ++ii) {
            bigEndianMem.putInt(ii*Integer.BYTES, 0xff << (8 * ii));
            littleEndianMem.putInt(ii*Integer.BYTES, 0xff << (8 * ii));
        }
        
        IntBuffer ib = bb.asIntBuffer();
        IntBuffer il = bl.asIntBuffer();
        
        for (int ii = 0; ii < 4; ++ii) {
            assertNotEquals(ib.get(ii), il.get(ii));
        }
    }

}
