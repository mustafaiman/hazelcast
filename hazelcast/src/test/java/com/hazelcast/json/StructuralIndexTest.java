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

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.query.impl.predicates.StructuralIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigInteger;

@Category({QuickTest.class, ParallelTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class StructuralIndexTest {


    @Test
    public void testSimpleJson() {
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString);
        System.out.println(jsonString);
        System.out.println(index);
        index.printQuoteIndex();
    }

    @Test
    public void testSimpleJson_extractValue() {
        JsonValue value = Json.object().add("a1", "v1");
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString);
        System.out.println(jsonString);
        System.out.println(index);
        System.out.println(index.findValueByPath("a1"));
    }

    @Test
    public void testNestedJson() {
        JsonValue value = Json.object()
                .add("a1", "v1")
                .add("a2", Json.object()
                        .add("b1", "v2")
                        .add("b2", "v3"))
                .add("a3", Json.object()
                        .add("c1", Json.object()
                                .add("d1", "v4")
                                .add("d2", "v5")));

        System.out.println(value.toString());
        String jsonString = value.toString();
        StructuralIndex index = new StructuralIndex(jsonString);
        System.out.println(index);
        System.out.println(index.findValueByPath("a3"));
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
