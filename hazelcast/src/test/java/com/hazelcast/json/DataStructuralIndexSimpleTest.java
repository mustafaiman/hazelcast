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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.misonparser.StructuralIndex;
import com.hazelcast.query.misonparser.StructuralIndexFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class DataStructuralIndexSimpleTest extends HazelcastTestSupport {

    private JsonObject createJsonObjectForPerson(String name, boolean status) {
        return Json.object()
                .add("name", name)
                .add("status", status);
    }

    private JsonObject createJsonObjectForPerson(String name, boolean status, int age) {
        return createJsonObjectForPerson(name, status)
                .add("age", age)
                .add("dummyField", "dumdum");
    }

    private Predicate equalPredicate(String attributeName, Comparable value) {
        return new EqualPredicate(attributeName, value);
    }

    private Predicate greaterLessPredicate(String attributeName, Comparable value, boolean equal, boolean less) {
        return new GreaterLessPredicate(attributeName, value, equal, less);
    }

    @Test
    public void testQueryEqual_boolean() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true).toString();
        String json2 = createJsonObjectForPerson("kamil", false).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        map.put(1, StructuralIndexFactory.create(json1, 3));
        map.put(2, StructuralIndexFactory.create(json2, 3));

        Collection<StructuralIndex> results = map.values(equalPredicate("status", false));
        assertEquals(1, results.size());
        assertEquals(json2, results.iterator().next().getSequence());
    }

    @Test
    public void testQueryEqual_string() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true).toString();
        String json2 = createJsonObjectForPerson("kamil", false).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        map.put(1, StructuralIndexFactory.create(json1, 3));
        map.put(2, StructuralIndexFactory.create(json2, 3));

        Collection<StructuralIndex> results = map.values(equalPredicate("name", "kamil"));
        assertEquals(1, results.size());
        assertEquals(json2, results.iterator().next().getSequence());
    }

    @Test
    public void testQueryLessThan() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        map.put(1, StructuralIndexFactory.create(json1, 3));
        map.put(2, StructuralIndexFactory.create(json2, 3));

        Collection<StructuralIndex> results = map.values(greaterLessPredicate("age", 27, false, true));
        assertEquals(1, results.size());
        assertEquals(json1, results.iterator().next().getSequence());
    }

    @Test
    public void testQueryLessThanEqual() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        StructuralIndex s1 = StructuralIndexFactory.create(json1, 3);
        StructuralIndex s2 = StructuralIndexFactory.create(json2, 3);

        map.put(1, s1);
        map.put(2, s2);

        Collection<StructuralIndex> results = map.values(greaterLessPredicate("age", 30, true, true));
        assertEquals(2, results.size());
        assertContains(results, s1);
        assertContains(results, s2);
    }
    @Test
    public void testQueryGreaterThan() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        map.put(1, StructuralIndexFactory.create(json1, 3));
        map.put(2, StructuralIndexFactory.create(json2, 3));

        Collection<StructuralIndex> results = map.values(greaterLessPredicate("age", 27, false, false));
        assertEquals(1, results.size());
        assertEquals(json2, results.iterator().next().getSequence());
    }

    @Test
    public void testQueryGreaterThanEqual() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        StructuralIndex s1 = StructuralIndexFactory.create(json1, 3);
        StructuralIndex s2 = StructuralIndexFactory.create(json2, 3);

        map.put(1, s1);
        map.put(2, s2);

        Collection<StructuralIndex> results = map.values(greaterLessPredicate("age", 26, true, false));
        assertEquals(2, results.size());
        assertContains(results, s1);
        assertContains(results, s2);
    }

    @Test
    public void testQueryGreaterThanEqual_noResult() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, StructuralIndex> map = instance.getMap(randomMapName());

        map.put(1, StructuralIndexFactory.create(json1, 3));
        map.put(2, StructuralIndexFactory.create(json2, 3));

        Collection<StructuralIndex> results = map.values(greaterLessPredicate("age", 31, true, false));
        assertEquals(0, results.size());
    }

    @Test
    public void testPortableTemp() {
        HazelcastInstance instance = createHazelcastInstance();
        IMap<Integer, AgaogluMyPortable> map = instance.getMap("agaogluMyMap");
        map.put(1, new AgaogluMyPortable(5));
        Collection<AgaogluMyPortable> result = map.values(Predicates.greaterEqual("agaogluMyInt", 5));
        assertEquals(1, result.size());
    }

    public class AgaogluMyPortable implements Portable {

        private int agaoglyMyInt;

        public AgaogluMyPortable(int agaoglyMyInt) {
            this.agaoglyMyInt = agaoglyMyInt;
        }

        public int getAgaoglyMyInt() {
            return agaoglyMyInt;
        }

        public void setAgaoglyMyInt(int agaoglyMyInt) {
            this.agaoglyMyInt = agaoglyMyInt;
        }

        @Override
        public int getFactoryId() {
            return 1;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("agaogluMyInt", agaoglyMyInt);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.agaoglyMyInt = reader.readInt("agaogluMyInt");
        }
    }
}
