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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.json.Json;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.json.AttributeIndex;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class AttributeIndexTest extends HazelcastTestSupport {

    @Test
    public void testWriteRead() {
        HazelcastInstance instance = createHazelcastInstance();

        String jsonString = Json.object().add("name", "aname").toString();

        AttributeIndex i1 = AttributeIndex.create(jsonString);

        Map<Integer, AttributeIndex> map = instance.getMap(randomName());
        map.put(1, i1);

        assertEquals(jsonString, map.get(1).asString());
    }

    @Test
    public void testPredicate() {
        HazelcastInstance instance = createHazelcastInstance();

        String jsonString = Json.object().add("name", "aname").toString();
        String jsonString2 = Json.object().add("name", "differentName").toString();

        AttributeIndex i1 = AttributeIndex.create(jsonString);
        AttributeIndex i2 = AttributeIndex.create(jsonString2);

        IMap<Integer, AttributeIndex> map = instance.getMap(randomName());
        map.put(1, i1);
        map.put(2, i2);

        Collection<AttributeIndex> coll = map.values(Predicates.equal("name", "differentName"));
        assertEquals(1, coll.size());
        assertContains(coll, i2);
    }

    @Test
    public void testPredicate_nested() {
        HazelcastInstance instance = createHazelcastInstance();

        String jsonString = Json.object()
                .add("address", Json.object()
                        .add("street", "astreet")
                        .add("number", 4)
                        .add("attrs", Json.object()
                                .add("a", true)
                                .add("b", false)
                                .add("c", true))
                        .add("country", "US"))
                .toString();
        String jsonString2 = Json.object()
                .add("address", Json.object()
                        .add("street", "bstreet")
                        .add("number", 9)
                        .add("attrs", Json.object()
                                .add("a", false)
                                .add("b", false)
                                .add("c", false))
                        .add("country", "US"))
                .toString();
        String jsonString3 = Json.object()
                .add("address", Json.object()
                        .add("street", "cstreet")
                        .add("number", 9)
                        .add("attrs", Json.object()
                                .add("a", true)
                                .add("b", false)
                                .add("c", false))
                        .add("country", "US"))
                .toString();

        AttributeIndex i1 = AttributeIndex.create(jsonString);
        AttributeIndex i2 = AttributeIndex.create(jsonString2);
        AttributeIndex i3 = AttributeIndex.create(jsonString3);

        IMap<Integer, AttributeIndex> map = instance.getMap(randomName());
        map.put(1, i1);
        map.put(2, i2);
        map.put(3, i3);

        Collection<AttributeIndex> coll = map.values(Predicates.equal("address.attrs.a", true));
        assertEquals(2, coll.size());
        assertContains(coll, i1);
        assertContains(coll, i3);
    }
}
