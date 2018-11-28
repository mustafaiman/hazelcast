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

import com.hazelcast.internal.json.JsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExperimentalJsonParser {

    private Map<String, List<Integer>> patternMap = new HashMap<String, List<Integer>>();
    private boolean isNative;
    private ByteBufferPool allocator;

    public ExperimentalJsonParser() {
        this.isNative = false;
    }

    public ExperimentalJsonParser(boolean isNative) {
        this.isNative = isNative;
        this.allocator = new ByteBufferPool();
    }

    public JsonValue findValue(String jsonString, String attributePath) {
        String[] attributeParts = attributePath.split("\\.");
        StructuralIndex index;
        if (isNative) {
            index = NativeStructuralIndexFactory.create(jsonString, attributeParts.length + 1, allocator);
        } else {
            index = StructuralIndexFactory.create(jsonString, attributeParts.length + 1);
        }
        List<Integer> pattern = patternMap.get(attributePath);
        if (pattern != null) {
            JsonValue speculatedValue = index.findValueByPattern(pattern, attributeParts);
            if (speculatedValue != null) {
                index.getLeveledIndex().dispose();
                return speculatedValue;
            }
        }
        pattern = index.findPattern(attributeParts);
        if (pattern == null) {
            index.getLeveledIndex().dispose();
            return null;
        }
        patternMap.put(attributePath, pattern);
        JsonValue res = index.findValueByPattern(pattern, attributeParts);
        index.getLeveledIndex().dispose();
        return res;

    }

    public JsonValue findValue(StructuralIndex index, String attributePath) {
        String[] attributeParts = attributePath.split("\\.");
        List<Integer> pattern = patternMap.get(attributePath);
        if (pattern != null) {
            JsonValue speculatedValue = index.findValueByPattern(pattern, attributeParts);
            if (speculatedValue != null) {
                index.getLeveledIndex().dispose();
                return speculatedValue;
            }
        }
        pattern = index.findPattern(attributeParts);
        if (pattern == null) {
            index.getLeveledIndex().dispose();
            return null;
        }
        patternMap.put(attributePath, pattern);
        JsonValue res = index.findValueByPattern(pattern, attributeParts);
        index.getLeveledIndex().dispose();
        return res;

    }


    public JsonValue findValueWithoutPattern(String jsonString, String attributePath) {
        String[] attributeParts = attributePath.split("\\.");
        StructuralIndex index;
        if (isNative) {
            index = NativeStructuralIndexFactory.create(jsonString, attributeParts.length + 1, allocator);
        } else {
            index = StructuralIndexFactory.create(jsonString, attributeParts.length + 1);
        }
        List<Integer> pattern = index.findPattern(attributeParts);
        JsonValue res = index.findValueByPattern(pattern, attributeParts);
        index.getLeveledIndex().dispose();
        return res;
    }


    public JsonValue findValueWithoutPattern(StructuralIndex index, String attributePath) {
        String[] attributeParts = attributePath.split("\\.");
        List<Integer> pattern = index.findPattern(attributeParts);
        JsonValue res = index.findValueByPattern(pattern, attributeParts);
        index.getLeveledIndex().dispose();
        return res;
    }
}
