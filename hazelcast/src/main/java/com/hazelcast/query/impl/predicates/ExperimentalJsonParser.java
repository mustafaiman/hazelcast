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

import com.hazelcast.internal.json.JsonValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExperimentalJsonParser {

    private Map<String, List<Integer>> patternMap = new HashMap<String, List<Integer>>();

    public JsonValue findValue(String jsonString, String attributePath) {
        StructuralIndex index = new StructuralIndex(jsonString);
        List<Integer> pattern = patternMap.get(attributePath);
        if (pattern != null) {
            JsonValue speculatedValue = index.findValueByPattern(pattern, attributePath);
            if (speculatedValue != null) {
                return speculatedValue;
            }
        }
        pattern = index.findPattern(attributePath);
        if (pattern == null) {
            return null;
        }
        patternMap.put(attributePath, pattern);
        return index.findValueByPattern(pattern, attributePath);

    }

    public JsonValue findValueWithoutPattern(String jsonString, String attributePath) {
        StructuralIndex index = new StructuralIndex(jsonString);
        List<Integer> pattern = index.findPattern(attributePath);
        return index.findValueByPattern(pattern, attributePath);
    }
}
