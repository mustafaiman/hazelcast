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

import com.hazelcast.query.impl.predicates.JsonEqualPredicate;
import com.hazelcast.query.impl.predicates.JsonEqualWithFilteringPredicate;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class EqualPredicateBenchmark extends AbstractJsonBenchmark {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public Object noFiltering(JsonState state) {
        return state.map.values(new JsonEqualPredicate("stringField", "queryvalue"));
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public Object withFiltering(JsonState state) {
        return state.map.values(new JsonEqualWithFilteringPredicate("stringField", "queryvalue"));
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(EqualPredicateBenchmark.class.getSimpleName())
                .forks(1)
                .warmupForks(1)
                .warmupIterations(8)
                .measurementIterations(8)
                .build();
        new Runner(opt).run();
    }
}
