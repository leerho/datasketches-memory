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

import java.nio.ByteBuffer;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("javadoc")
public class ZeroCapacityTest {

  @SuppressWarnings({ "unused", "resource" })
  @Test
  public void checkZeroCapacity() throws Exception {
    WritableMemory.allocate(0);
    Memory.wrap(new byte[0]);
    Memory.wrap(ByteBuffer.allocate(0));
    Memory.wrap(ByteBuffer.allocateDirect(0));
    Memory mem = WritableMemory.allocate(8);
    mem.region(0, 0);
    WritableHandle wh = null;
    try {
      wh = WritableMemory.allocateDirect(0);
      Assert.fail();
    } catch (IllegalArgumentException ignore) {
      if (wh != null) { wh.close(); }
      // expected
    }
  }

}
