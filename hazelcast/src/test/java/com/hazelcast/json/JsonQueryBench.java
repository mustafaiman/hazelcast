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


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Random;

import static com.hazelcast.json.Benches.IN_MEMORY_FORMAT;
import static com.hazelcast.json.Benches.QUERY_ATTRIBUTE_NAME;

public class JsonQueryBench {

    private final static int MAP_SIZE = 5000;

    @State(Scope.Benchmark)
    public static class Hazel {

        Random random;
        HazelcastInstance instance;
        IMap map;

        @Setup(Level.Trial)
        public void setup() {
            Config config = new Config();
            config.getMapConfig("default").setInMemoryFormat(IN_MEMORY_FORMAT);
            instance = Hazelcast.newHazelcastInstance(config);
            map = instance.getMap("test");
            random = new Random();

            fillMap(map);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            instance.shutdown();
        }

        private void fillMap(IMap map) {
            for (int i = 0; i < MAP_SIZE; i++) {
                JsonObject o = Json.object();
                o.set(QUERY_ATTRIBUTE_NAME, "" + random.nextInt(10) + random.nextInt(10));
                o.set("a", random.nextDouble());
                o.set("b", random.nextDouble());
                o.set("c", random.nextDouble());
                o.set("d", random.nextDouble());
                o.set("e", random.nextDouble());
                o.set("f", random.nextDouble());
                map.put(i, o);
            }
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void bensc(Hazel hazel) {
        hazel.map.keySet(Predicates.equal(QUERY_ATTRIBUTE_NAME, "11"));
    }
}
