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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonSchemaDataGetter extends Getter {

    private InternalSerializationService ss;
    private final Map<String, List<Integer>> patternMap = new HashMap<String, List<Integer>>();

    public JsonSchemaDataGetter(InternalSerializationService serializationService) {
        super(null);
        ss = serializationService;
    }

    @Override
    Object getValue(Object obj) throws Exception {
        return null;
    }

    @Override
    Object getValue(Object obj, String attributePath) throws Exception {
        Data data = (Data) obj;
        BufferObjectDataInput is = ss.createObjectDataInput(data);
        List<Integer> pattern = patternMap.get(attributePath);
        if (pattern == null) {
            int backup = is.position();
            pattern = findPattern(is, attributePath);
            is.position(backup);
        }
        if (pattern == null) {
            return null;
        }
        patternMap.put(attributePath, pattern);
        return findValueWithPattern(is, pattern);
    }

    public JsonValue findValueWithPattern(BufferObjectDataInput is, List<Integer> pattern) throws IOException {
        int offset = 0;//is.position();
        int startOfMetadata = is.readInt();
        is.position(offset + startOfMetadata);

        try {
            for (int i = 0; i < pattern.size(); i++) {
                int x = pattern.get(i);
                int numberOfEl = is.readShort();
                assumeGreater(0, numberOfEl);
                assumeLE(numberOfEl, x);
                is.skipBytes(x * 8);
                is.skipBytes(4);
                int descriptionAddress = is.readInt();
                is.position(offset + descriptionAddress);
            }
            short type = is.readShort();
            assume(0, type);
            is.position(offset + is.readInt());
            return readValue(is);
        } catch (AssumptionException e) {
            return null;
        }
    }

    public List<Integer> findPattern(BufferObjectDataInput is, String attributePath) throws IOException {
        String[] path = attributePath.split("\\.");
        List<Integer> pattern = new ArrayList<Integer>(path.length);

        int offset = 0;//is.position();
        int startOfMetadata = is.readInt();
        is.position(offset + startOfMetadata);
        for (int i = 0; i < path.length; i++) {
            int numberOfElements = is.readShort();
            if (numberOfElements <= 0) {
                return null;
            }
            boolean foundCurrent = false;
            for (int j = 0; j < numberOfElements; j++) {
                int nameAddress = is.readInt();
                int descriptionAddres = is.readInt();
                int nextMetadataAddress = is.position();
                is.position(offset + nameAddress);
                try {
                    assumeAttributeName(path[i], is);
                    is.position(offset + descriptionAddres);
                    pattern.add(j);
                    foundCurrent = true;
                    break;
                } catch (AssumptionException e) {
                    is.position(offset + nextMetadataAddress);
                }
            }
            if (!foundCurrent) {
                return null;
            }
        }
        short type = is.readShort();
        assume(0, type);
        return pattern;
    }

    protected JsonValue readValue(BufferObjectDataInput is) throws IOException {
        byte i = is.readByte();
        switch (i) {
            case '"':
                return readString(is);
            case '-':
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
                return readNumber(is, i);
            case 't':
                is.skipBytes(3);
                return Json.TRUE;
            case 'f':
                is.skipBytes(4);
                return Json.FALSE;
            case 'n':
                is.skipBytes(3);
                return Json.NULL;
            default:
                throw new HazelcastException("not implemented yet");
        }
    }

    private JsonValue readString(BufferObjectDataInput is) throws IOException {
        StringBuilder builder = new StringBuilder();
        byte b;
        while (true) {
            b = is.readByte();
            switch (b) {
                case '"':
                    return Json.value(builder.toString());
                default:
                    if (b < 0) {
                        builder.append(Bits.readUtf8Char(is, b));
                    } else {
                        builder.append((char) b);
                    }
                    break;
            }
        }
    }

    private JsonValue readNumber(BufferObjectDataInput is, byte firstByte) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append((char)firstByte);
        byte b;
        while (true) {
            try {
                b = is.readByte();
            } catch (IOException e) {
                return Json.valueAsNumber(builder.toString());
            }
            if (b > 34 && b < 123) {
                builder.append((char)b);
            } else {
                return Json.valueAsNumber(builder.toString());
            }
        }
    }

    private void assumeLE(int limit, int actual) {
        if (limit < actual) {
            throw new AssumptionException();
        }
    }

    private void assumeGreater(int limit, int actual) {
        if (limit >= actual) {
            throw new AssumptionException();
        }
    }

    private void assume(int expected, int actual) {
        if (expected != actual) {
            throw new AssumptionException();
        }
    }

    private void assume(short expected, short actual) {
        if (expected != actual) {
            throw new AssumptionException();
        }
    }

    private void assumeAttributeName(String attributeName, ObjectDataInput is) throws IOException {
        byte[] nameBytes = attributeName.getBytes(Charset.forName("UTF8"));
        assumeQuote(is);
        for (int i = 0; i < nameBytes.length; i++) {
            if (nameBytes[i] != is.readByte()) {
                throw new AssumptionException();
            }
        }
        assumeQuote(is);
    }

    private void assumeQuote(ObjectDataInput is) throws IOException {
        if (is.readByte() != '"') {
            throw new AssumptionException();
        }
    }

    @Override
    Class getReturnType() {
        return null;
    }

    @Override
    boolean isCacheable() {
        return false;
    }

    class AssumptionException extends RuntimeException {

    }
}
