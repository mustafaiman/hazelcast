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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.misonparser.StructuralIndex;
import com.hazelcast.query.misonparser.StructuralIndexFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class OverheadComparisonTest extends HazelcastTestSupport {

    private SerializationServiceV1 serializationService;

    private JsonValue jsonValue = Json.object()
            .add("name", "aName")
            .add("age", 5)
            .add("address", Json.object()
                    .add("street", "aStreetName")
                    .add("city", "aCity"));

    @Before
    public void setup() {
        DefaultSerializationServiceBuilder defaultSerializationServiceBuilder = new DefaultSerializationServiceBuilder();
        serializationService = defaultSerializationServiceBuilder
                .setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void testStructuralIndexOverhead() {


        String jsonText = jsonValue.toString();
        StructuralIndex structuralIndex = StructuralIndexFactory.create(jsonText, 4);
        Data dataText = serializationService.toData(jsonText);
        Data dataStructuralIndex = serializationService.toData(structuralIndex);

        int textHeapCost = dataText.getHeapCost();
        int sindexHeapCost = dataStructuralIndex.getHeapCost();

        System.out.println(textHeapCost);
        System.out.println(sindexHeapCost);

        assertTrue(
                String.format("Plain string cost: %d, structural index cost: %d. " +
                        "Overhead should not be more than 20 percent",
                        textHeapCost, sindexHeapCost),
                sindexHeapCost <= ((double)textHeapCost * 120 / 100));
    }
}
