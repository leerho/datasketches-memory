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

/**
 * Defines the relative positional API.
 * This is different from and simpler than Java ByteBuffer positional API.
 * <ul><li>All based on longs instead of ints.</li>
 * <li>Eliminated "mark". Rarely used and confusing with its silent side effects.</li>
 * <li>The invariants are {@code 0 <= start <= position <= end <= capacity}.</li>
 * <li>It always starts up as (0, 0, capacity, capacity).</li>
 * <li>You set (start, position, end) in one call with
 * {@link #setStartPositionEnd(long, long, long)}</li>
 * <li>Position can be set directly or indirectly when using the positional get/put methods.
 * <li>Added incrementPosition(long), which is much easier when you know the increment.</li>
 * <li>This approach eliminated a number of methods and checks, and has no unseen side effects,
 * e.g., mark being invalidated.</li>
 * <li>Clearer method naming (IMHO).</li>
 * </ul>
 * @author Lee Rhodes
 */
public interface BaseBuffer extends Resource {

  /**
   * Increments the current position by the given increment.
   * Asserts that the resource is valid and that the positional invariants are not violated,
   * otherwise, if asserts are enabled throws an {@link AssertionError}.
   * @param increment the given increment
   * @return BaseBuffer
   */
  BaseBuffer incrementPosition(long increment);

  /**
   * Increments the current position by the given increment.
   * Checks that the resource is valid and that the positional invariants are not violated,
   * otherwise throws an {@link IllegalArgumentException}.
   * @param increment the given increment
   * @return BaseBuffer
   */
  BaseBuffer incrementAndCheckPosition(final long increment);

  /**
   * Gets the end position
   * @return the end position
   */
  long getEnd();

  /**
   * Gets the current position
   * @return the current position
   */
  long getPosition();

  /**
   * Gets start position
   * @return start position
   */
  long getStart();

  /**
   * The number of elements remaining between the current position and the end position
   * @return {@code (end - position)}
   */
  long getRemaining();

  /**
   * Returns true if there are elements remaining between the current position and the end position
   * @return {@code (end - position) > 0}
   */
  boolean hasRemaining();

  /**
   * Resets the current position to the start position,
   * This does not modify any data.
   * @return BaseBuffer
   */
  BaseBuffer resetPosition();

  /**
   * Sets the current position.
   * Asserts that the positional invariants are not violated,
   * otherwise, if asserts are enabled throws an {@link AssertionError}.
   * @param position the given current position.
   * @return BaseBuffer
   */
  BaseBuffer setPosition(long position);

  /**
   * Sets the current position.
   * Checks that the positional invariants are not violated,
   * otherwise, throws an {@link IllegalArgumentException}.
   * @param position the given current position.
   * @return BaseBuffer
   */
  BaseBuffer setAndCheckPosition(long position);

  /**
   * Sets start position, current position, and end position.
   * Asserts that the positional invariants are not violated,
   * otherwise, if asserts are enabled throws an {@link AssertionError}.
   * @param start the start position in the buffer
   * @param position the current position between the start and end
   * @param end the end position in the buffer
   * @return BaseBuffer
   */
  BaseBuffer setStartPositionEnd(long start, long position, long end);

  /**
   * Sets start position, current position, and end position.
   * Checks that the positional invariants are not violated,
   * otherwise, throws an {@link IllegalArgumentException}.
   * @param start the start position in the buffer
   * @param position the current position between the start and end
   * @param end the end position in the buffer
   * @return BaseBuffer
   */
  BaseBuffer setAndCheckStartPositionEnd(long start, long position, long end);

}
