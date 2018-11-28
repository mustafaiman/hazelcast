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

public class StructuralIndex {

    protected LeveledColonPositionList leveledIndex;
    protected String sequence;
    protected int maxNesting;

    protected StructuralIndex() {

    }

    public StructuralIndex(String sequence, long[] array, int len) {
        this.sequence = sequence;
        this.maxNesting = array.length / len;
        this.leveledIndex = new ArrayLeveledColonPositionsList(array, len);
    }

    public StructuralIndex(String sequence, int maxNesting, ArrayLeveledColonPositionsList leveledIndex) {
        this.sequence = sequence;
        this.maxNesting = maxNesting;
        this.leveledIndex = leveledIndex;
    }

    public String getSequence() {
        return sequence;
    }

    public LeveledColonPositionList getLeveledIndex() {
        return leveledIndex;
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
}
