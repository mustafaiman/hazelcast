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

package com.hazelcast.query.impl.getters;

import com.hazelcast.json.JsonArray;
import com.hazelcast.json.JsonValue;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.util.Iterator;

import static com.hazelcast.util.EmptyStatement.ignore;

public final class JsonGetter extends Getter {

    public static final JsonGetter INSTANCE = new JsonGetter();
    private static final String DELIMITER = "\\.";

    private JsonGetter() {
        super(null);
    }

    @Override
    Object getValue(Object obj) {
        return convertFromJsonValue((JsonValue) obj);
    }

    //TODO: reduce n-path complexity here
    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    Object getValue(Object obj, String attributePath) {
        JsonValue value = (JsonValue) obj;
        if (value.isObject()) {
            JsonValue jsonValue = value.asObject().get(attributePath);
            if (jsonValue != null) {
                return convertFromJsonValue(jsonValue);
            }
        }

        String[] paths = getPath(attributePath);
        if (value.isObject()) {
            for (int i = 0; i < paths.length; i++) {
                String path = paths[i];
                if (value == null) {
                    return null;
                }
                try {
                    if (path.charAt(path.length() - 1) == ']') {
                        int from = 0;
                        int leftBracket = path.indexOf('[', from);
                        if (leftBracket > 0) {
                            String qualifier = path.substring(from, leftBracket);
                            value = value.asObject().get(qualifier);
                        }
                        while (leftBracket > -1) {
                            int rightBracket = path.indexOf(']', leftBracket);
                            JsonArray valueArray = value.asArray();
                            String indexText = path.substring(leftBracket + 1, rightBracket);
                            if (indexText.equals("any")) {
                                return getMultiValue(valueArray, paths, i + 1);
                            } else {
                                int index = Integer.parseInt(indexText);
                                value = valueArray.get(index);
                            }
                            from = rightBracket + 1;
                            leftBracket = path.indexOf('[', from);
                        }
                    } else {
                        value = value.asObject().get(path);
                    }
                } catch (IndexOutOfBoundsException ex) {
                    ignore(ex);
                    return null;
                }
            }
            return convertFromJsonValue(value);
        }
        return null;
    }

    private MultiResult getMultiValue(JsonArray arr, String[] paths, int index) {
        MultiResult<Object> multiResult = new MultiResult<Object>();
        Iterator<JsonValue> it = arr.iterator();
        String attr = paths.length > index ? paths[index] : null;
        while (it.hasNext()) {
            JsonValue value = it.next();
            if (attr != null) {
                try {
                    JsonValue found = value.asObject().get(attr);
                    if (found != null) {
                        multiResult.add(convertFromJsonValue(found));
                    }
                } catch (UnsupportedOperationException ex) {
                    ignore(ex);
                }
            } else {
                multiResult.add(convertFromJsonValue(value));
            }
        }
        return multiResult;
    }

    public static Object convertFromJsonValue(JsonValue value) {
        if (value == null) {
            return null;
        } else if (value.isNumber()) {
            return value.asDouble();
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isNull()) {
            return null;
        } else if (value.isString()) {
            return value.asString();
        }
        throw new HazelcastSerializationException("Unknown Json type: " + value);
    }

    @Override
    Class getReturnType() {
        return JsonValue.class;
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    private String[] getPath(String attributePath) {
        return attributePath.split(DELIMITER);
    }
}
