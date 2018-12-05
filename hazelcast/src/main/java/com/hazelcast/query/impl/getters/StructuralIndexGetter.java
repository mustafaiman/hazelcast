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

package com.hazelcast.query.impl.getters;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.misonparser.ByteArrayLeveledPositionList;
import com.hazelcast.query.misonparser.DataStructuralIndex;
import com.hazelcast.query.misonparser.ExperimentalJsonParser;
import com.hazelcast.query.misonparser.StructuralIndex;

public class StructuralIndexGetter extends Getter {

    private final InternalSerializationService serializationService;

    protected static final ExperimentalJsonParser misonParser = new ExperimentalJsonParser();

    public StructuralIndexGetter(InternalSerializationService serializationService) {
        super(null);
        this.serializationService = serializationService;
    }

    public StructuralIndexGetter(Getter parent, InternalSerializationService serializationService) {
        super(parent);
        this.serializationService = serializationService;
    }

    @Override
    Object getValue(Object obj) throws Exception {
        throw new HazelcastException("not implemented");
    }

    @Override
    Object getValue(Object obj, String attributePath) {

        try {

            Data dataObject = (Data) obj;
            BufferObjectDataInput dataInput = serializationService.createObjectDataInput(dataObject);
            int stringLen = dataInput.readInt();
            int offset = dataInput.position();
            dataInput.skipBytes(stringLen * 2);
            int len = dataInput.readInt();
            int longs = dataInput.readInt();
            int pos = dataInput.position();

            DataStructuralIndex dataStructuralIndex = new DataStructuralIndex(dataInput, offset, stringLen, new ByteArrayLeveledPositionList(dataInput, len, pos));

            return misonParser.findValue(dataStructuralIndex, attributePath);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }

    @Override
    Class getReturnType() {
        return StructuralIndex.class;
    }

    @Override
    boolean isCacheable() {
        return false;
    }
}
