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
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.query.misonparser.ByteBufferPool;
import com.hazelcast.query.misonparser.ExperimentalJsonParser;
import com.hazelcast.query.misonparser.StructuralIndex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExperimentalParserBenchmark {

    protected static final int K = 1000;
    protected static final int M = 1000000;
    private static final int JSON_OBJECT_COUNT = 100 * K;
    private static final int INT_BOUND = M;
    private static final Random random = new Random();

    @State(Scope.Benchmark)
    public static class JsonState {

        public List<String> jsonStrings = new ArrayList<String>();
        public List<StructuralIndex> structuralIndices = new ArrayList<StructuralIndex>();
        public ExperimentalJsonParser parser = new ExperimentalJsonParser();
        public ExperimentalJsonParser nativeParser = new ExperimentalJsonParser(true);
        public ByteBufferPool genericAllocator = new ByteBufferPool();

        @Setup(Level.Trial)
        public void setup() {
            for (int i = 0; i < JSON_OBJECT_COUNT; i++) {
                String jsonString = createJsonObject().toString();
                jsonStrings.add(jsonString);
                structuralIndices.add(new StructuralIndex(jsonString, 5));
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {

        }
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void conventionalParser(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            JsonValue value = Json.parse(jsonObject).asObject().get("objectField").asObject().get("1");
//            blackhole.consume(value);
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void experimentalParser(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            JsonValue value = state.parser.findValue(jsonObject,"objectField.1");
//            blackhole.consume(value);
//        }
//    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void experimentalParser_preIndexed(JsonState state, Blackhole blackhole) {
        for (StructuralIndex jsonObject: state.structuralIndices) {
            JsonValue value = state.parser.findValue(jsonObject,"objectField.1");
            blackhole.consume(value);
        }
    }

//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void experimentalNativeParser(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            JsonValue value = state.nativeParser.findValue(jsonObject,"objectField.1");
//            blackhole.consume(value);
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void experimentalParserWithoutSpeculation(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            JsonValue value = state.parser.findValueWithoutPattern(jsonObject,"objectField.1");
//            blackhole.consume(value);
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void aa_onlyCreateTokenIndexesNative(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            StructuralIndex index = new NativeStructuralIndex(jsonObject, 5, state.genericAllocator);
//            blackhole.consume(index);
//            index.dispose();
//        }
//    }
//
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    public void ab_onlyCreateTokenIndexes(JsonState state, Blackhole blackhole) {
//        for (String jsonObject: state.jsonStrings) {
//            StructuralIndex index = new StructuralIndex(jsonObject, null);
//            blackhole.consume(index);
//        }
//    }

    private static String randomString() {
        if (random.nextInt(10) < 9) {
            return "" + random.nextDouble();
        } else {
            return "queryvalue";
        }
    }

    private static JsonObject createTinyJsonObject() {
        return Json.object()
                .add("objectField", Json.object()
                        .add("1", "a")
                        .add("2", "b")
                        .add("3", "c"));
    }

    public static JsonObject createJsonObject() {
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
                        .add("1", "a")
                        .add("2", "b")
                        .add("3", "c"));
    }

    private static JsonObject createHugeJsonObject() {
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
                .add("doubleField", random.nextDouble())
                .add("floatField", random.nextFloat())
                .add("someLargeField", Json.object()
                        .add("ff1", "tr1")
                        .add("ff2", random.nextFloat())
                        .add("ff3", "" + random.nextInt()))
                .add("longField", random.nextLong())
                .add("intField", INT_BOUND)
                .add("stringField", randomString())
                .add("objectField3", Json.object()
                        .add("1", 1)
                        .add("2", 2)
                        .add("3", 3))
                .add("objectField", Json.object()
                        .add("1", "a")
                        .add("2", "b")
                        .add("3", "c"));
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ExperimentalParserBenchmark.class.getSimpleName())
                .forks(1)
                .warmupForks(1)
                .warmupIterations(8)
                .measurementIterations(8)
                .build();
        new Runner(opt).run();
    }
}
