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
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicates;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Random;

import static com.hazelcast.json.Benches.CLUSTER_SIZE;
import static com.hazelcast.json.Benches.IN_MEMORY_FORMAT;
import static com.hazelcast.json.Benches.MAP_SIZE;
import static com.hazelcast.json.Benches.QUERY_ATTRIBUTE_NAME;

public class PortableQueryBench {

    @State(Scope.Benchmark)
    public static class Hazel {

        Random random;
        HazelcastInstance[] instances;
        IMap map;

        @Setup(Level.Trial)
        public void setup() {
            Config config = new Config();
            config.getMapConfig("default").setInMemoryFormat(IN_MEMORY_FORMAT);
            config.getSerializationConfig().addPortableFactory(MyPortableFactory.F_ID, new MyPortableFactory());
            instances = new HazelcastInstance[CLUSTER_SIZE];
            for (int i = 0; i < CLUSTER_SIZE; i++) {
                instances[i] = Hazelcast.newHazelcastInstance(config);
            }
            map = instances[0].getMap("test");
            random = new Random();

            fillMap(map);
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            for (int i = 0; i < CLUSTER_SIZE; i++) {
                instances[i].shutdown();
            }
        }

        private void fillMap(IMap map) {
            for (int i = 0; i < MAP_SIZE; i++) {
                MyPortable o = new MyPortable(
                        "" + random.nextInt(10) + random.nextInt(10) + random.nextInt(10),
                        random.nextDouble(),
                        random.nextDouble(),
                        random.nextDouble(),
                        random.nextDouble(),
                        random.nextDouble(),
                        random.nextDouble());
                map.put(i, o);
            }
        }
    }

    public static class MyPortable implements Portable {

        private String stringVam;
        private double a;
        private double b;
        private double c;
        private double d;
        private double e;
        private double f;

        public MyPortable() {

        }

        public MyPortable(String stringVam, double a, double b, double c, double d, double e, double f) {
            this.stringVam = stringVam;
            this.a = a;
            this.b = b;
            this.c = c;
            this.d = d;
            this.e = e;
            this.f = f;
        }

        @Override
        public int getFactoryId() {
            return MyPortableFactory.F_ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeDouble("a", this.a);
            writer.writeDouble("b", this.b);
            writer.writeDouble("c", this.c);
            writer.writeDouble("d", this.d);
            writer.writeDouble("e", this.e);
            writer.writeDouble("f", this.f);
            writer.writeUTF(QUERY_ATTRIBUTE_NAME, this.stringVam);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            this.a = reader.readDouble("a");
            this.b = reader.readDouble("b");
            this.c = reader.readDouble("c");
            this.d = reader.readDouble("d");
            this.e = reader.readDouble("e");
            this.f = reader.readDouble("f");
            this.stringVam = reader.readUTF(QUERY_ATTRIBUTE_NAME);
        }
    }

    public static class MyPortableFactory implements PortableFactory {

        static final int F_ID = 1;

        @Override
        public Portable create(int classId) {
            return new MyPortable();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 2)
    @Warmup(iterations = 1)
    public void bensc(Hazel hazel) {
        hazel.map.keySet(Predicates.equal(QUERY_ATTRIBUTE_NAME, "1112"));
    }
}
