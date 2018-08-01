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

package com.hazelcast.query.impl;


import com.hazelcast.json.Json;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.query.DefaultIndexProvider;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(QuickTest.class)
public class IndexJsonTest {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testJsonIndex() {
        Indexes is = new Indexes(ss, new DefaultIndexProvider(), Extractors.empty(), true, copyBehavior);
        Index dIndex = is.addOrGetIndex("age", false);
        Index boolIndex = is.addOrGetIndex("active", false);
        Index strIndex = is.addOrGetIndex("name", false);

        for (int i = 0; i < 1000; i++) {
            Data key = ss.toData(i);
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            is.saveEntryIndex(new QueryEntry(ss, key, Json.parse(jsonString), Extractors.empty()), null);
        }

        assertEquals(1, dIndex.getRecords(10).size());
        assertEquals(0, dIndex.getRecords(-1).size());
        assertEquals(1000, strIndex.getRecords("sancar").size());
        assertEquals(500, boolIndex.getRecords(true).size());
        assertEquals(500, is.query(new AndPredicate(new EqualPredicate("name", "sancar"), new EqualPredicate("active", "true"))).size());
        assertEquals(299, is.query(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1000, is.query(new SqlPredicate("name == sancar")).size());
    }

}