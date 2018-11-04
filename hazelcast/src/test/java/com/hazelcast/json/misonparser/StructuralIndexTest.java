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

package com.hazelcast.json.misonparser;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.query.misonparser.SIMDHelper;
import com.hazelcast.query.misonparser.ByteBufferPool;
import com.hazelcast.query.misonparser.ExperimentalJsonParser;
import com.hazelcast.query.misonparser.BufferLeveledColonPositionList;
import com.hazelcast.query.misonparser.NativeStructuralIndex;
import com.hazelcast.query.misonparser.StructuralIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Category({QuickTest.class, ParallelTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class StructuralIndexTest {
    @Test
    public void testSIMDHelper() {
        JsonValue value = Json.object()
                .add("a", "b")
                .add("s", "f")
                .add("\\f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", Json.object()
                                .add("f", Json.object()
                                        .add("t", "3"))))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"))
                .add("s", "f")
                .add("\\f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", Json.object()
                                .add("f", Json.object()
                                        .add("t", "3"))))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"))
                .add("s", "f")
                .add("\\f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", "3"))
                .add("f", Json.object()
                        .add("t", Json.object()
                                .add("f", Json.object()
                                        .add("t", "3"))))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"))
                .add("f", Json.object().add("t", "3"));
//        value = Json.object()
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3)
//                .add("a", 3);
        String text = value.toString();
        System.out.println(text.length());
        for (int i = 0; i < text.length();i++) {
            System.out.print(i % 10 == 0 ? (i / 10) % 10: " ");
        }
        System.out.println();
        for (int i = 0; i < text.length(); i++) {
            System.out.print(i % 10);
        }
        System.out.println();
        System.out.println(text);

        int indexLength = (((text.length() - 1)  >> 7) + 1) << 1;


        int queryDepth = 5;
        ByteBufferPool allocator = new ByteBufferPool();
        ByteBuffer buf = allocator.allocateBuffer(indexLength * queryDepth);
        SIMDHelper.createCharacterIndexes(text, queryDepth, buf);

        System.out.println(indexLength);
        for (int i = 0; i < queryDepth; i++) {
            for (int j = 0; j < indexLength * 8; j++) {
                System.out.print(buf.get(i*indexLength*8 + j) + " ");
            }
            System.out.println();
        }
        System.out.println("-------------");
        BufferLeveledColonPositionList l = new BufferLeveledColonPositionList(buf, indexLength);

        System.out.println(Arrays.toString(l.getColons(1, 1, 120).toArray()));

    }

    @Test
    public void testNativeCrashIssue() {
        JsonValue value = ExperimentalParserBenchmark.createJsonObject();
        String jsonString = value.toString();
        System.out.println(jsonString);
        for (int i = 0; i < 1000; i++) {
            ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
            JsonValue result = parser.findValue(jsonString, "objectField.1");
            assertEquals("a", result.asString());
        }
    }

    @Test
    public void testNativeTiny() {
        JsonValue value = Json.object()
                .add("objectField", Json.object()
                        .add("1", 1)
                        .add("2", 2)
                        .add("3", 3));
        NativeStructuralIndex index = new NativeStructuralIndex(value.toString(), 2, new ByteBufferPool());
    }

    @Test
    public void testSimpleJson() {
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 2);
        System.out.println(jsonString);
        System.out.println(index);
    }

    @Test
    public void testSimpleJson_extractValue_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        System.out.println(jsonString);
        assertEquals(Json.value("v1"), parser.findValue(jsonString,"a1"));
    }

    @Test
    public void testSimpleJson_extractValue() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 1);
        System.out.println(jsonString);
        System.out.println(index);
        assertEquals(Json.value("v1"), parser.findValue(jsonString,"a1"));
    }

    @Test
    public void testSimpleJson_extractValue_byPattern2_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object().add("a1", "v1").add("a2", "v2");
        String jsonString = value.toString();
        assertEquals(Json.value("v2"), parser.findValue(jsonString, "a2"));
    }

    @Test
    public void testSimpleJson_extractValue_byPattern2() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object().add("a1", "v1").add("a2", "v2");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 2);
        System.out.println(jsonString);
        System.out.println(index);
        assertEquals(Json.value("v2"), parser.findValue(jsonString, "a2"));
    }

    @Test
    public void testSimpleJson_extractValue_byPattern_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        assertEquals(Json.value("v1"), parser.findValue(jsonString,"a1"));
    }

    @Test
    public void testSimpleJson_extractValue_byPattern() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 2);
        System.out.println(jsonString);
        System.out.println(index);
        assertEquals(Json.value("v1"), parser.findValue(jsonString,"a1"));
    }

    @Test
    public void testNestedJson_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        String jsonString = value.toString();
        System.out.println(jsonString.length());
        for (int i = 0; i < jsonString.length();i++) {
            System.out.print(i % 10 == 0 ? (i / 10) % 10: " ");
        }
        System.out.println();
        for (int i = 0; i < jsonString.length(); i++) {
            System.out.print(i % 10);
        }
        System.out.println();
        System.out.println(jsonString);
        assertEquals(Json.value("uy"), parser.findValue(jsonString, "a3.c1.d2"));
    }

    @Test
    public void testNestedJson() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 4);
        System.out.println(index);
        System.out.println();
        assertEquals(Json.value("uy"), parser.findValue(jsonString, "a3.c1.d2"));
        assertEquals(Json.value("uy"), parser.findValue(jsonString, "a3.c1.d2"));
    }

    @Test
    public void testNestedJsonWithEscapedQuoute() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "u" + '"' + "y")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 5);
        assertEquals(Json.value("u\"y"), parser.findValue(jsonString, "a3.c1.d2"));
    }

    @Test
    public void testNestedJson_byPattern_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 5);
        System.out.println(index);
        System.out.println();
        assertEquals(Json.value("uy"), parser.findValue(jsonString,"a3.c1.d2"));
    }

    @Test
    public void testNestedJson_byPattern() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 5);
        System.out.println(index);
        System.out.println();
        assertEquals(Json.value("uy"), parser.findValue(jsonString,"a3.c1.d2"));
    }

    @Test
    public void testNestedJson_byPattern2() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        assertEquals(Json.value("v4"), parser.findValue(jsonString,"a3.c1.d1"));
    }

    @Test
    public void testNestedJson_byPattern3_native() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser(true);
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 5);
        System.out.println(index);
        System.out.println();
        assertEquals(Json.value("v2"), parser.findValue(jsonString,"a2.b1"));
    }

    @Test
    public void testNestedJson_byPattern3() {
        ExperimentalJsonParser parser = new ExperimentalJsonParser();
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "uy")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString, 5);
        System.out.println(index);
        System.out.println();
        assertEquals(Json.value("v2"), parser.findValue(jsonString,"a2.b1"));
    }

    @Test
    public void testBigInteger() {
        BigInteger colons = new BigInteger(":::::::::::::::::::::::::::".getBytes());
        BigInteger str = new BigInteger("{\"dsfddf\": 5}".getBytes());
        System.out.println(colons.toString(16));
        System.out.println(str.toString(16));
        System.out.println(colons.and(str).toString(16));
    }
}
