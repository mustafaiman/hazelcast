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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.hazelcast.json.JsonArray;
import com.hazelcast.json.JsonValue;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.IOException;
import java.util.Iterator;

import static com.hazelcast.internal.serialization.impl.HeapData.HEAP_DATA_OVERHEAD;
import static com.hazelcast.util.EmptyStatement.ignore;

public class JsonGetter extends Getter {

    private static final String DELIMITER = "\\.";

    private JsonFactory factory = new JsonFactory();

    public JsonGetter() {
        super(null);
    }

    public JsonGetter(Getter parent) {
        super(parent);
    }

    @Override
    Object getValue(Object obj) {
        return convertFromJsonValue((JsonValue) obj);
    }

    @Override
    Object getValue(Object obj, String attributePath) throws IOException {
        if (obj instanceof Data) {
            return getValueFromBinary((Data) obj, attributePath);
        } else if (obj instanceof JsonValue) {
            return getValueFromObject(obj, attributePath);
        } else {
            throw new HazelcastSerializationException("object is not JsonValue or Data");
        }
    }

    protected Object getValueFromBinary(Data data, String attributePath) throws IOException {
        String[] paths = getPath(attributePath);
        JsonParser parser = factory.createParser(data.toByteArray(), HEAP_DATA_OVERHEAD + 4, data.dataSize() - 4);
        int index = 0;
        while (index < paths.length && parser.getCurrentToken() != JsonToken.END_OBJECT) {
            String path = paths[index];
            parser.nextValue();
            String currentName = parser.getCurrentName();
            if (path.equals(currentName)) {
                index++;
            }
        }
        Object ret = null;
        if (index == paths.length) {
            ret = convertJsonTokenToValue(parser);
        }
        parser.close();
        return ret;
    }

    private Object convertJsonTokenToValue(JsonParser parser) throws IOException {
        int token = parser.getCurrentTokenId();
        switch (token) {
            case JsonTokenId.ID_STRING:
                return parser.getValueAsString();
            case JsonTokenId.ID_NUMBER_INT:
                return parser.getIntValue();
            case JsonTokenId.ID_NUMBER_FLOAT:
                return parser.getValueAsDouble();
            case JsonTokenId.ID_TRUE:
                return true;
            case JsonTokenId.ID_FALSE:
                return false;
            default:
                return null;
        }
    }

    protected Object getValueFromObject(Object obj, String attributePath) {
        String[] paths = getPath(attributePath);
        JsonValue value = (JsonValue) obj;
        if (value.isObject()) {
            for (int i = 0; i < paths.length; i++) {
                String path = paths[i];
                if (value == null) {
                    return null;
                }
                try {
                    if (path.endsWith("]")) {
                        int leftBracket = path.indexOf('[');
                        if (leftBracket > 0) {
                            String qualifier = path.substring(0, leftBracket);
                            value = value.asObject().get(qualifier);
                        }
                        JsonArray valueArray = value.asArray();
                        String indexText = path.substring(leftBracket + 1, path.length() - 1);
                        if (indexText.equals("any")) {
                            return getMultiValue(valueArray, paths, i + 1);
                        } else {
                            int index = Integer.parseInt(indexText);
                            value = valueArray.get(index);
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

    MultiResult getMultiValue(JsonArray arr, String[] paths, int index) {
        MultiResult multiResult = new MultiResult();
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
        return true;
    }

    private String[] getPath(String attributePath) {
        return attributePath.split(DELIMITER);
    }
}
