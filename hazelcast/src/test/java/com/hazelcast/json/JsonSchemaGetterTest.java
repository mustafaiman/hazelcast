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

package com.hazelcast.json;

import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.query.impl.getters.JsonSchemaDataGetter;
import com.hazelcast.query.json.JsonSchemaCreator;
import com.hazelcast.query.json.JsonSchemaNonLeafDescription;
import com.hazelcast.query.json.JsonSchemaSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class JsonSchemaGetterTest extends AbstractJsonSchemaTest {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testFindPattern_whenOneOuterTwoInner() throws IOException {
        String jsonString = Json.object().add("firstObject", Json.object().add("firstAttribute", Json.object().add("extraAttr", "extraName")).add("secondAttribute", 5)).toString();

        BufferObjectDataInput input = (BufferObjectDataInput) serializeAndCreateInputStream(jsonString);

        JsonSchemaDataGetter getter = new JsonSchemaDataGetter(serializationService);

        int backupPos = input.position();
        List<Integer> pattern = getter.findPattern(input, "firstObject.secondAttribute");
        assertEquals(0, (int)pattern.get(0));
        assertEquals(1, (int)pattern.get(1));

        input.position(backupPos);
        pattern = getter.findPattern(input, "firstObject.firstAttribute.extraAttr");
        assertEquals(0, (int)pattern.get(0));
        assertEquals(0, (int)pattern.get(1));
        assertEquals(0, (int)pattern.get(2));
    }

    @Test
    public void testFindPattern_whenOneLevel_manyAttributes_lastElement() throws IOException {
        String jsonString = Json.object()
                .add("a1", "v1")
                .add("a2", "v2")
                .add("a3", "v3")
                .add("a4", "v4")
                .add("a5", "v5")
                .toString();

        BufferObjectDataInput input = (BufferObjectDataInput) serializeAndCreateInputStream(jsonString);

        JsonSchemaDataGetter getter = new JsonSchemaDataGetter(serializationService);

        List<Integer> pattern = getter.findPattern(input, "a5");
        assertEquals(4, (int)pattern.get(0));

    }

    @Test
    public void testFindPattern_whenOnlyValue_shouldReturnNull() throws IOException {
        String jsonString = Json.value("a")
                .toString();

        BufferObjectDataInput input = (BufferObjectDataInput) serializeAndCreateInputStream(jsonString);

        JsonSchemaDataGetter getter = new JsonSchemaDataGetter(serializationService);

        List<Integer> pattern = getter.findPattern(input, "x");
        assertNull(pattern);

    }

    @Test
    public void testFindValue_whenOneOuterTwoInner_withPattern() throws IOException {
        String jsonString = Json.object().add("firstObject", Json.object().add("firstAttribute", Json.object().add("extraAttr", "extraName")).add("secondAttribute", 5)).toString();

        printWithGuides(jsonString);
        BufferObjectDataInput input = (BufferObjectDataInput) serializeAndCreateInputStream(jsonString);

        JsonSchemaDataGetter getter = new JsonSchemaDataGetter(serializationService);

        int backupPos = input.position();
        List<Integer> pattern = getter.findPattern(input, "firstObject.secondAttribute");
        input.position(backupPos);
        assertEquals(Json.value(5), getter.findValueWithPattern(input, pattern));

        input.position(backupPos);
        pattern = getter.findPattern(input, "firstObject.firstAttribute.extraAttr");
        input.position(backupPos);
        assertEquals(Json.value("extraName"), getter.findValueWithPattern(input, pattern));
    }

    protected ObjectDataInput serializeAndCreateInputStream(String jsonString) throws IOException {
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        return serializationService.createObjectDataInput(output.toByteArray());
    }
}
