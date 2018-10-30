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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class StructuralIndex {

    private OpenBitSet colonIndex;
    private OpenBitSet leftBraceIndex;
    private OpenBitSet rightBraceIndex;
    private OpenBitSet quoteIndex;
    private OpenBitSet backslashIndex;
    private List<OpenBitSet> levelIndex = new ArrayList<OpenBitSet>(5);
    private CharSequence sequence;

    public StructuralIndex(String sequence) {

        this.sequence = sequence;
        colonIndex = new OpenBitSet(sequence.length());
        leftBraceIndex = new OpenBitSet(sequence.length());
        rightBraceIndex = new OpenBitSet(sequence.length());
        quoteIndex = new OpenBitSet(sequence.length());
        backslashIndex = new OpenBitSet(sequence.length());

        createIndexes(sequence);
        createLeveledIndex();
    }

    public String toString(int level) {
        StringBuilder builder = new StringBuilder();
        for (int j = 0; j < getLevel(level).length(); j++) {
            builder.append(getLevel(level).get(j) ? "+": "-");
        }
        builder.append('\t').append(level);
        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < levelIndex.size(); i++) {
            builder.append(toString(i));
            builder.append('\n');
        }
        return builder.toString();
    }

    public void printQuoteIndex() {
        for (int i = 0; i < quoteIndex.size(); i++) {
            System.out.print(quoteIndex.get(i) ? "+": "-");
        }
        System.out.println();
    }

    private void createLeveledIndex() {
        OpenBitSet inter = new OpenBitSet(colonIndex.length());
        Stack<Integer> stack = new Stack<Integer>();
        for (int rPos = rightBraceIndex.nextSetBit(0); rPos >= 0; rPos = rightBraceIndex.nextSetBit(rPos+1)) {
            stack.clear();
            for (int lPos = leftBraceIndex.nextSetBit(0); lPos >= 0; lPos = leftBraceIndex.nextSetBit(lPos+1)) {
                if (lPos < rPos) {
                    stack.push(lPos);
                } else {
                    break;
                }
            }
            int lPos = stack.pop();
            leftBraceIndex.fastClear(lPos);
            int level = stack.size();
            inter.set(lPos, rPos + 1);
            inter.and(colonIndex);
            colonIndex.clear(lPos, rPos + 1);
            getLevel(level).or(inter);
            inter.clear(lPos, rPos + 1);
        }
    }

    private OpenBitSet getLevel(int level) {
        if (level < levelIndex.size()) {
            return levelIndex.get(level);
        }
        while (levelIndex.size() <= level) {
            levelIndex.add(new OpenBitSet(sequence.length()));
        }
        return levelIndex.get(level);
    }

    private void createIndexes(CharSequence text) {
        int x = 0;
        for (int i = 0; i < colonIndex.getNumWords(); i++) {
            long bits = 1;
            for (int j = 0; j < 64 && x < text.length(); j++) {
                char c = text.charAt(x);
                switch (c) {
                    case ':':
                        colonIndex.getBits()[i] |= bits;
                        break;
                    case '"':
                        quoteIndex.getBits()[i] |= bits;
                        break;
                    case '{':
                        leftBraceIndex.getBits()[i] |= bits;
                        break;
                    case '}':
                        rightBraceIndex.getBits()[i] |= bits;
                        break;
                    case '\\':
                        backslashIndex.getBits()[i] |= bits;
                        break;
                }
                x++;
                bits <<= 1;
            }
        }
        OpenBitSet shiftedBackslash = new OpenBitSet(sequence.length());
        long remainder = 0;
        for (int i = shiftedBackslash.getNumWords() - 1; i > 0; i--) {
            shiftedBackslash.getBits()[i] = ~((backslashIndex.getBits()[i] << 1) | remainder);
            remainder = shiftedBackslash.getBits()[i-1] & 0x8000;
        }
        shiftedBackslash.getBits()[0] = ~((backslashIndex.getBits()[0] << 1));

        quoteIndex.intersect(shiftedBackslash);
//        for (int i = 0; i < text.length(); i++) {
//            char c = text.charAt(i);
//            switch (c) {
//                case ':':
//                    colonIndex.fastSet(i);
//                    break;
//                case '"':
//                    quoteIndex.fastSet(i);
//                    break;
//                case '{':
//                    leftBraceIndex.fastSet(i);
//                    break;
//                case '}':
//                    rightBraceIndex.fastSet(i);
//                    break;
//                case '\\':
//                    backslashIndex.fastSet(i);
//                    break;
//            }
//        }
    }

    private boolean attributeNameMatches(int colonLoc, String attributeName) {
        int endQuote = quoteIndex.prevSetBit(colonLoc);
        int startQuote = quoteIndex.prevSetBit(endQuote - 1);
        if (endQuote - startQuote - 1 == attributeName.length()) {
            int index = 0;
            for (int i = startQuote + 1; i < endQuote; i++) {
                if (attributeName.charAt(index) != sequence.charAt(i)) {
                    return false;
                }
                index++;
            }
            return true;
        } else {
            return false;
        }
    }


    public JsonValue findValueByPattern(List<Integer> pattern, String attributePath) {
        String[] parts = attributePath.split("\\.");
        int start = 0;
        int end = sequence.length();
        int level = 0;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            OpenBitSet levelMap = getLevel(level);
            for (int j = 0, colonLoc = levelMap.nextSetBit(start); j <= pattern.get(i) && colonLoc >= 0 && colonLoc < end; j++, colonLoc = levelMap.nextSetBit(colonLoc + 1)) {
                start = colonLoc;
            }
            if (attributeNameMatches(start, part)) {
                int tempEnd = levelMap.nextSetBit(start + 1);
                if (tempEnd != -1) {
                    end = tempEnd;
                }
                level++;
            } else {
                return null;
            }
        }
        return readJsonValue(start);
    }

    public List<Integer> findPattern(String attributePath) {
        String[] parts = attributePath.split("\\.");
        ArrayList<Integer> pattern = new ArrayList<Integer>();
        int start = 0;
        int end = sequence.length();
        int level = 0;
        for (String part: parts) {
            OpenBitSet levelMap = getLevel(level);
            boolean found = false;
            int i = -1;
            for (int colonLoc = levelMap.nextSetBit(start); colonLoc >= 0 && colonLoc < end; colonLoc = levelMap.nextSetBit(colonLoc + 1)) {
                i++;
                if (attributeNameMatches(colonLoc, part)) {
                    start = colonLoc;
                    int tempEnd = levelMap.nextSetBit(start + 1);
                    if (tempEnd != -1) {
                        end = tempEnd;
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

    public JsonValue doNothing() {
        if (levelIndex.size() > 100000) {
            throw new RuntimeException();
        }
        return Json.NULL;
    }

    private JsonValue readJsonValue(int start) {
        start = skipWhitespace(start+1);
        char c = sequence.charAt(start);
        switch (c) {
            case 't':
                return Json.TRUE;
            case 'f':
                return Json.FALSE;
            case '"':
                return Json.value(readString(start, quoteIndex.nextSetBit(start + 1)));
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

    private String readString(int start, int end) {
        return sequence.subSequence(start + 1, end).toString();
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

}
