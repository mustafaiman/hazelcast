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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.JsonEqualPredicate;
import com.hazelcast.query.impl.predicates.JsonEqualWithFilteringPredicate;
import com.hazelcast.query.impl.predicates.JsonGreaterLessPredicate;
import com.hazelcast.query.impl.predicates.JsonGreaterLessWithFilteringPredicate;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@Category({ParallelTest.class, QuickTest.class})
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@RunWith(Parameterized.class)
public class JsonSimpleTest extends HazelcastTestSupport {

    @Parameters(name = "withFiltering:{0}")
    public static Collection<Object> parameters() {
        return asList(new Object[]{
                true,
                false
        });
    }

    @Parameter
    public boolean withFiltering;

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
        if (withFiltering) {
            return new JsonEqualWithFilteringPredicate(attributeName, value);
        } else {
            return new JsonEqualPredicate(attributeName, value);
        }
    }

    private Predicate greaterLessPredicate(String attributeName, Comparable value, boolean equal, boolean less) {
        if (withFiltering) {
            return new JsonGreaterLessWithFilteringPredicate(attributeName, value, equal, less);
        } else {
            return new JsonGreaterLessPredicate(attributeName, value, equal, less);
        }
    }

    @Test
    public void testQueryEqual_boolean() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true).toString();
        String json2 = createJsonObjectForPerson("kamil", false).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(equalPredicate("status", false));
        assertEquals(1, results.size());
        assertContains(results, json2);
    }

    @Test
    public void testQueryEqual_string() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true).toString();
        String json2 = createJsonObjectForPerson("kamil", false).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(equalPredicate("name", "kamil"));
        assertEquals(1, results.size());
        assertContains(results, json2);
    }

    @Test
    public void testQueryLessThan() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(greaterLessPredicate("age", 27, false, true));
        assertEquals(1, results.size());
        assertContains(results, json1);
    }

    @Test
    public void testQueryLessThanEqual() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(greaterLessPredicate("age", 30, true, true));
        assertEquals(2, results.size());
        assertContains(results, json1);
        assertContains(results, json2);
    }
    @Test
    public void testQueryGreaterThan() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(greaterLessPredicate("age", 27, false, false));
        assertEquals(1, results.size());
        assertContains(results, json2);
    }

    @Test
    public void testQueryGreaterThanEqual() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(greaterLessPredicate("age", 26, true, false));
        assertEquals(2, results.size());
        assertContains(results, json1);
        assertContains(results, json2);
    }

    @Test
    public void testQueryGreaterThanEqual_noResult() {
        HazelcastInstance instance = createHazelcastInstance();
        String json1 = createJsonObjectForPerson("mustafa", true, 26).toString();
        String json2 = createJsonObjectForPerson("kamil", false, 30).toString();

        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(1, json1);
        map.put(2, json2);

        Collection<String> results = map.values(greaterLessPredicate("age", 31, true, false));
        assertEquals(0, results.size());
    }

}
