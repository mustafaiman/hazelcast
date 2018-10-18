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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Random;

public abstract class AbstractJsonBenchmark {

    protected static final int K = 1000;
    protected static final int M = 1000000;
    private static final int JSON_OBJECT_COUNT = 100 * K;
    private static final int INT_BOUND = M;
    private static final Random random = new Random();

    @State(Scope.Benchmark)
    public static class JsonState {

        public IMap<Integer, String> map;

        private HazelcastInstance[] instances = new HazelcastInstance[2];

        @Setup(Level.Trial)
        public void setup() {
            instances[0] = Hazelcast.newHazelcastInstance();
            instances[1] = Hazelcast.newHazelcastInstance();
            map = instances[0].getMap("testMap");
            for (int i = 0; i < JSON_OBJECT_COUNT; i++) {
                map.put(i, createJsonObject().toString());
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {
            for (HazelcastInstance instance: instances) {
                instance.shutdown();
            }
        }
    }

    private static String randomString() {
        if (random.nextInt(10) < 9) {
            return "" + random.nextDouble();
        } else {
            return "queryvalue";
        }
    }

    private static JsonObject createJsonObject() {
        return Json.object()
                .add("doubleField", random.nextDouble())
                .add("floatField", random.nextFloat())
                .add("someLargeField", Json.object()
                        .add("ff1", "tr1")
                        .add("ff2", random.nextFloat())
                        .add("ff3", "" + random.nextInt()))
                .add("longField", random.nextLong())
                .add("intField", INT_BOUND)
                .add("stringField", randomString())
                .add("objectField", Json.object()
                        .add("1", 1)
                        .add("2", 2)
                        .add("3", 3));
    }
}
