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
import com.hazelcast.nio.BufferObjectDataOutput;
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

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class JsonSchemaSerializerTest extends AbstractJsonSchemaTest {

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testValue() throws IOException {
        String jsonString = Json.value("abc").toString();
        printWithGuides(jsonString);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        byte[] bytes = output.toByteArray();

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(i + " " + (int)bytes[i]);
        }
    }

    @Test
    public void testSimple() throws IOException {
        String jsonString = Json.object().add("name", "aName").toString();
        printWithGuides(jsonString);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        byte[] bytes = output.toByteArray();

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(i + " " + (int)bytes[i]);
        }
    }

    @Test
    public void testSimpleNested() throws IOException {
        String jsonString = Json.object().add("firstObject", Json.object().add("firstAttribute", 3)).toString();
        printWithGuides(jsonString);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        byte[] bytes = output.toByteArray();

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(i + " " + (int)bytes[i]);
        }
    }

    @Test
    public void testOneOuterTwoInner() throws IOException {
        String jsonString = Json.object().add("firstObject", Json.object().add("firstAttribute", 3).add("secondAttribute", 5)).toString();
        printWithGuides(jsonString);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        byte[] bytes = output.toByteArray();

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(i + " " + (int)bytes[i]);
        }
    }

    @Test
    public void testOneOuterTwoInner_2() throws IOException {
        String jsonString = Json.object().add("firstObject", Json.object().add("firstAttribute", Json.object().add("extraAttr", "extraName")).add("secondAttribute", 5)).toString();
        printWithGuides(jsonString);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        JsonParser parser = createParserFromString(jsonString);

        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);
        JsonSchemaSerializer.writeTo(output, jsonString, description);

        byte[] bytes = output.toByteArray();

        for (int i = 0; i < bytes.length; i++) {
            System.out.println(i + " " + (int)bytes[i]);
        }
    }

}
