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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class StructuralIndex {

    public static StructuralIndex createStructuralIndex(String text, int maxNesting, boolean isNative, ByteBufferPool allocator) {
        if (isNative) {
            return new NativeStructuralIndex(text, maxNesting, allocator);
        } else {
            return new StructuralIndex(text, maxNesting);
        }
    }

    protected LeveledColonPositionList leveledIndex;
    protected String sequence;
    protected int maxNesting;

    protected StructuralIndex() {

    }

    public StructuralIndex(String sequence, int maxNesting) {
        this.sequence = sequence;
        this.maxNesting = maxNesting;
        createLeveledIndex();
    }

    public void dispose() {

    }

    protected void fillIndexes(long[] colonIndex, long[] leftBraceIndex, long[] rightBraceIndex, long[] quoteIndex, long[] backslashIndex) {
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

    protected void maskEscapedQuotes(long[] backslashIndex, long[] quoteIndex) {
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

    protected void createLeveledIndex() {
        int indexLength = (((sequence.length() - 1)  >> 7) + 1) << 1;

        long[] colonIndex = new long[indexLength];
        long[] leftBraceIndex = new long[indexLength];
        long[] rightBraceIndex = new long[indexLength];
        long[] quoteIndex = new long[indexLength];
        long[] backslashIndex = new long[indexLength];

        fillIndexes(colonIndex, leftBraceIndex, rightBraceIndex, quoteIndex, backslashIndex);

        maskEscapedQuotes(backslashIndex, quoteIndex);

        long[] coll = new long[indexLength * maxNesting];
        buildLeveledColonMap(coll, maxNesting, indexLength, colonIndex, leftBraceIndex, rightBraceIndex);
        leveledIndex = new ArrayLeveledColonPositionsList(coll, indexLength);
    }

    private void buildLeveledColonMap(long[] indexCollection, int indexMaxNesting, int n, long[] colonIndex, long[] lBraceIndex, long[] rBraceIndex) {
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

    protected boolean attributeNameMatches(int colonLoc, String attributeName) {
        colonLoc--;
        while (Character.isWhitespace(sequence.charAt(colonLoc)))
            colonLoc--;
        int index = colonLoc - attributeName.length();
        if (index < 1 || sequence.charAt(colonLoc) != '"' || sequence.charAt(index - 1) != '"') {
            return false;
        }
        for (int i = 0; i < attributeName.length(); i++) {
            if (attributeName.charAt(i) != sequence.charAt(index)) {
                return false;
            }
            index++;
        }
        return true;
    }

    public JsonValue findValueByPattern(List<Integer> pattern, String[] parts) {
        int start = 0;
        int end = sequence.length();
        int level = 1;
        int colonLoc = -1;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            List<Integer> colonLocations = leveledIndex.getColons(level, start, end);
            int colonIndex = pattern.get(i);
            if (colonLocations.size() <= colonIndex) {
                return null;
            }
            colonLoc = colonLocations.get(colonIndex);
            if (attributeNameMatches(colonLoc, part)) {
                start = colonLoc + 1;
                if (colonIndex + 1 < colonLocations.size()) {
                    end = colonLocations.get(colonIndex+1);
                }
                level++;
            } else {
                return null;
            }
        }
        return readJsonValue(colonLoc);
    }

    public List<Integer> findPattern(String[] parts) {
        ArrayList<Integer> pattern = new ArrayList<Integer>(10);
        int start = 0;
        int end = sequence.length();
        int level = 1;
        for (String part: parts) {
            List<Integer> colonLocations = leveledIndex.getColons(level, start, end);
            boolean found = false;
            for (int i = 0; i < colonLocations.size(); i++) {
                int colonLoc = colonLocations.get(i);
                if (attributeNameMatches(colonLoc, part)) {
                    start = colonLoc + 1;
                    if (i + 1 < colonLocations.size() ) {
                        end = colonLocations.get(i+1);
                    }
                    level++;
                    found = true;
                    pattern.add(i);
                    break;
                }
            }
            if (!found) {
                return null;
            }
        }
        return pattern;
    }

    protected JsonValue readJsonValue(int start) {
        start = skipWhitespace(start+1);
        char c = sequence.charAt(start);
        switch (c) {
            case 't':
                return Json.TRUE;
            case 'f':
                return Json.FALSE;
            case '"':
                return Json.value(readString(start));
            case '-':
                return Json.value(readNumber(start + 1, false));
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                return Json.value(readNumber(start, true));
            default:
                return Json.NULL;

        }
    }

    private String readString(int start) {
        StringBuilder builder = new StringBuilder();
        start++;
        while (sequence.charAt(start) != '"') {
            builder.append(sequence.charAt(start));
            start++;
        }
        return builder.toString();
    }

    private Double readNumber(int start, boolean positive) {
        int end = start;
        while ((sequence.charAt(end) <= '9' && sequence.charAt(end) >= '0') || sequence.charAt(end) == '.') {
            end++;
        }
        return Double.parseDouble(sequence.subSequence(start, end).toString());
    }

    private int skipWhitespace(int start) {
        while (start < sequence.length()) {
            if (Character.isWhitespace(sequence.charAt(start))) {
                start++;
            } else {
                return start;
            }
        }
        return sequence.length();
    }

    private static class StackItem {

        public StackItem(long val, int index) {
            this.val = val;
            this.index = index;
        }

        public long val;
        public int index;
    }

}
