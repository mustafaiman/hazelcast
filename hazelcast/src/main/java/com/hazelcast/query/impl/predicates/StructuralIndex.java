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

import java.util.BitSet;
import java.util.Stack;

public class StructuralIndex {

    private BitSet colonIndex = new BitSet();
    private BitSet leftBraceIndex = new BitSet();
    private BitSet rightBraceIndex = new BitSet();
    private BitSet quoteIndex = new BitSet();
    private BitSet backslashIndex = new BitSet();
    private BitSet[] levelIndex = new BitSet[5];
    private CharSequence sequence;

    public StructuralIndex(String sequence) {

        this.sequence = sequence;
        for (int i = 0; i < levelIndex.length; i++) {
            levelIndex[i] = new BitSet();
        }
        createColonIndex(sequence);
        createLeftBraceIndex(sequence);
        createRightBraceIndex(sequence);
        createQuoteIndex(sequence);
        createLeveledIndex();
    }

    public String toString(int level) {
        StringBuilder builder = new StringBuilder();
        for (int j = 0; j < levelIndex[level].length(); j++) {
            builder.append(levelIndex[level].get(j) ? "+": "-");
        }
        builder.append('\t').append(level);
        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < levelIndex.length; i++) {
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

    private void createStructuralCharacterIndex(CharSequence text, char structuralChar, BitSet indexBitSet) {
        for (int i = ((String)text).indexOf(structuralChar); i != -1; i = ((String)text).indexOf(structuralChar, i + 1)) {
            indexBitSet.set(i);
        }
    }

    private void createLeveledIndex() {
        for (int rPos = rightBraceIndex.nextSetBit(0); rPos >= 0; rPos = rightBraceIndex.nextSetBit(rPos+1)) {
            Stack<Integer> stack = new Stack<Integer>();
            for (int lPos = leftBraceIndex.nextSetBit(0); lPos >= 0; lPos = leftBraceIndex.nextSetBit(lPos+1)) {
                if (lPos < rPos) {
                    stack.push(lPos);
                } else {
                    break;
                }
            }
            int lPos = stack.pop();
            leftBraceIndex.clear(lPos);
            int level = stack.size();
            BitSet inter = new BitSet(colonIndex.length());
            inter.set(lPos, rPos + 1);
            inter.and(colonIndex);
            colonIndex.clear(lPos, rPos + 1);
            levelIndex[level].or(inter);
        }
    }

    private void createColonIndex(CharSequence text) {
        createStructuralCharacterIndex(text, ':', colonIndex);
    }

    private void createLeftBraceIndex(CharSequence text) {
        createStructuralCharacterIndex(text, '{', leftBraceIndex);
    }

    private void createRightBraceIndex(CharSequence text) {
        createStructuralCharacterIndex(text, '}', rightBraceIndex);
    }

    private void createQuoteIndex(CharSequence text) {
        createStructuralCharacterIndex(text, '"', quoteIndex);
    }

    private void createBackslashIndex(CharSequence text) {
        createStructuralCharacterIndex(text, '\\', backslashIndex);
    }

    private boolean attributeNameMatches(int colonLoc, String attributeName) {
        int endQuote = quoteIndex.previousSetBit(colonLoc);
        int startQuote = quoteIndex.previousSetBit(endQuote - 1);
        int index = 0;
        for (int i = startQuote + 1; i < endQuote; i++) {
            if (attributeName.charAt(index) != sequence.charAt(i)) {
                return false;
            }
            index++;
        }
        return index == attributeName.length();
    }

    public JsonValue findValueByPath(String attributePath) {
        String[] parts = attributePath.split("\\.");
        int start = 0;
        int end = sequence.length();
        int level = 0;
        for (String part: parts) {
            BitSet levelMap = levelIndex[level];
            boolean found = false;
            for (int colonLoc = levelMap.nextSetBit(start); colonLoc >= 0 && colonLoc < end; colonLoc = levelMap.nextSetBit(colonLoc + 1)) {
                if (attributeNameMatches(colonLoc, part)) {
                    start = colonLoc;
                    int tempEnd = levelMap.nextSetBit(start + 1);
                    if (tempEnd != -1) {
                        end = tempEnd;
                    }
                    level++;
                    found = true;
                    break;
                }
            }
            if (!found) {
                return null;
            }
        }
        start = skipWhitespace(start+1);
        char c = sequence.charAt(start);
        switch (c) {
            case 't':
                return Json.TRUE;
            case 'f':
                return Json.FALSE;
            case '"':
                return Json.value(readString(start + 1));
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
        int end = start;
        while (sequence.charAt(end) != '"') {
            end++;
        }
        return sequence.subSequence(start, end).toString();
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
