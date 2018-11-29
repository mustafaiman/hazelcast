/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.query.misonparser;

import java.util.Stack;

public class StructuralIndexFactory {

    private static class StackItem {

        public StackItem(long val, int index) {
            this.val = val;
            this.index = index;
        }

        public long val;
        public int index;
    }

    public static StructuralIndex create(String sequence, int maxNesting) {
        int indexLength = (((sequence.length() - 1)  >> 7) + 1) << 1;

        long[] colonIndex = new long[indexLength];
        long[] leftBraceIndex = new long[indexLength];
        long[] rightBraceIndex = new long[indexLength];
        long[] quoteIndex = new long[indexLength];
        long[] backslashIndex = new long[indexLength];

        fillIndexes(sequence, colonIndex, leftBraceIndex, rightBraceIndex, quoteIndex, backslashIndex);

        maskEscapedQuotes(backslashIndex, quoteIndex);

        long[] coll = new long[indexLength * maxNesting];
        buildLeveledColonMap(coll, maxNesting, indexLength, colonIndex, leftBraceIndex, rightBraceIndex);
        ArrayLeveledColonPositionsList leveledIndex = new ArrayLeveledColonPositionsList(coll, indexLength);
        return new StructuralIndex(sequence, leveledIndex);
    }

    protected static void fillIndexes(String sequence, long[] colonIndex, long[] leftBraceIndex, long[] rightBraceIndex, long[] quoteIndex, long[] backslashIndex) {
        int x = 0;
        for (int i = 0; i < colonIndex.length; i++) {
            long bits = 1;
            for (int j = 0; j < 64 && x < sequence.length(); j++) {
                char c = sequence.charAt(x);
                switch (c) {
                    case ':':
                        colonIndex[i] |= bits;
                        break;
                    case '"':
                        quoteIndex[i] |= bits;
                        break;
                    case '{':
                        leftBraceIndex[i] |= bits;
                        break;
                    case '}':
                        rightBraceIndex[i] |= bits;
                        break;
                    case '\\':
                        backslashIndex[i] |= bits;
                        break;
                }
                x++;
                bits <<= 1;
            }
        }
    }

    protected static void maskEscapedQuotes(long[] backslashIndex, long[] quoteIndex) {
        int indexLength = backslashIndex.length;
        long[] shiftedBackslash = new long[indexLength];
        long remainder = 0;
        for (int i = indexLength - 1; i > 0; i--) {
            shiftedBackslash[i] = ~((backslashIndex[i] << 1) | remainder);
            remainder = shiftedBackslash[i-1] & 0x8000;
        }
        shiftedBackslash[0] = ~((backslashIndex[0] << 1));
        for (int i = 0; i < indexLength; i++) {
            quoteIndex[i] &= shiftedBackslash[i];
        }
    }

    private static void buildLeveledColonMap(long[] indexCollection, int indexMaxNesting, int n, long[] colonIndex, long[] lBraceIndex, long[] rBraceIndex) {
        Stack<StackItem> S = new Stack<StackItem>();
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < indexMaxNesting; j++) {
                indexCollection[j*n+i] = colonIndex[i];
            }
        }

        for (int i = 0; i < n; i++) {
            long mleft = lBraceIndex[i];
            long mright = rBraceIndex[i];
            long mrightbit = 0;
            do {
                mrightbit = BitOperations.extractFirstSetBit(mright);
                long mleftbit = BitOperations.extractFirstSetBit(mleft);
                while (mleftbit != 0 && (mrightbit == 0 || mleftbit < mrightbit)) {
                    StackItem it = new StackItem(mleftbit, i);
                    S.push(it);
                    mleft = BitOperations.removeFirstSetBit(mleft);
                    mleftbit = BitOperations.extractFirstSetBit(mleft);
                }
                if (mrightbit != 0) {
                    int stackSize = S.size();
                    StackItem it = S.pop();
                    int j = it.index;
                    mleftbit = it.val;
                    if (0 < stackSize && stackSize <= indexMaxNesting) {
                        if (i == j) {
                            indexCollection[(stackSize - 1)*n+i] = indexCollection[(stackSize - 1)*n+i] & ~(mrightbit - mleftbit);
                        } else {
                            indexCollection[(stackSize - 1)*n+j] = indexCollection[(stackSize - 1)*n+j] & (mleftbit - 1);
                            indexCollection[(stackSize - 1)*n+i] = indexCollection[(stackSize - 1)*n+i] & ~(mrightbit - 1);
                            int k;
                            for (k = j + 1; k < i; k++) {
                                indexCollection[(stackSize-1)*n+k] = 0;
                            }
                        }
                    }
                    mright = BitOperations.removeFirstSetBit(mright);
                }
            } while (mrightbit != 0);
        }
    }

}
