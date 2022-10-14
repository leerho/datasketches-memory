/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.memory;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;

import static org.testng.Assert.assertEquals;
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
            mem.putChar(ii, (char) ('a' + ii));
        }
        
        CharBuffer cb = bb.asCharBuffer();
        for (int ii = 0; ii < 26; ++ii) {
            assertEquals(cb.get(ii), 'a'+ii);
        }
    }
}
