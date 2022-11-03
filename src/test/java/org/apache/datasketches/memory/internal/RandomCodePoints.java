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

import java.util.Random;

public class RandomCodePoints {
  private Random rand; //
  private static final int ALL_CP = Character.MAX_CODE_POINT + 1;
  private static final int MIN_SUR = Character.MIN_SURROGATE;
  private static final int MAX_SUR = Character.MAX_SURROGATE;

  /**
   * @param deterministic if true, configure java.util.Random with a fixed seed.
   */
  public RandomCodePoints(final boolean deterministic) {
    rand = deterministic ? new Random(0) : new Random();
  }

  /**
   * Fills the given array with random valid Code Points from 0, inclusive, to
   * <i>Character.MAX_CODE_POINT</i>, inclusive. The surrogate range, which is
   * from <i>Character.MIN_SURROGATE</i>, inclusive, to
   * <i>Character.MAX_SURROGATE</i>, inclusive, is always <u>excluded</u>.
   *
   * @param cpArr the array to fill
   */
  public final void fillCodePointArray(final int[] cpArr) {
    fillCodePointArray(cpArr, 0, ALL_CP);
  }

  /**
   * Fills the given array with random valid Code Points from <i>startCP</i>,
   * inclusive, to <i>endCP</i>, exclusive. The surrogate range, which is from
   * <i>Character.MIN_SURROGATE</i>, inclusive, to <i>Character.MAX_SURROGATE</i>,
   * inclusive, is always <u>excluded</u>.
   *
   * @param cpArr   the array to fill
   * @param startCP the starting Code Point, included.
   * @param endCP   the ending Code Point, excluded. This value cannot exceed
   *                0x110000.
   */
  public final void fillCodePointArray(final int[] cpArr, final int startCP, final int endCP) {
    final int arrLen = cpArr.length;
    final int numCP = Math.min(endCP, 0X110000) - Math.min(0, startCP);
    int idx = 0;
    while (idx < arrLen) {
      final int cp = startCP + rand.nextInt(numCP);
      if ((cp >= MIN_SUR) && (cp <= MAX_SUR)) {
        continue;
      }
      cpArr[idx++] = cp;
    }
  }

  /**
   * Return a single valid random Code Point from 0, inclusive, to
   * <i>Character.MAX_CODE_POINT</i>, inclusive. The surrogate range, which is
   * from <i>Character.MIN_SURROGATE</i>, inclusive, to
   * <i>Character.MAX_SURROGATE</i>, inclusive, is always <u>excluded</u>.
   *
   * @return a single valid random CodePoint.
   */
  public final int getCodePoint() {
    return getCodePoint(0, ALL_CP);
  }

  /**
   * Return a single valid random Code Point from <i>startCP</i>, inclusive, to
   * <i>endCP</i>, exclusive. The surrogate range, which is from
   * <i>Character.MIN_SURROGATE</i>, inclusive, to <i>Character.MAX_SURROGATE</i>,
   * inclusive, is always <u>excluded</u>.
   *
   * @param startCP the starting Code Point, included.
   * @param endCP   the ending Code Point, excluded. This value cannot exceed
   *                0x110000.
   * @return a single valid random CodePoint.
   */
  public final int getCodePoint(final int startCP, final int endCP) {
    final int numCP = Math.min(endCP, 0X110000) - Math.min(0, startCP);
    while (true) {
      final int cp = startCP + rand.nextInt(numCP);
      if ((cp < MIN_SUR) || (cp > MAX_SUR)) {
        return cp;
      }
    }
  }
  } //End class RandomCodePoints