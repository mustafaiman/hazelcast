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

package com.hazelcast.query.json;

import com.hazelcast.nio.BufferObjectDataOutput;

import java.io.IOException;

public class JsonSchemaLeafDescription extends JsonSchemaDescription {

    private int valueStartLocation;

    public JsonSchemaLeafDescription(JsonSchemaNonLeafDescription parent) {
        super(parent);
    }

    @Override
    public void writeTo(BufferObjectDataOutput out, int stringIndex, int descriptionIndex) throws IOException {
        out.writeShort(0);
        out.writeInt(stringIndex + valueStartLocation);
    }

    public int getValueStartLocation() {
        return valueStartLocation;
    }

    public void setValueStartLocation(int valueStartLocation) {
        this.valueStartLocation = valueStartLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        JsonSchemaLeafDescription that = (JsonSchemaLeafDescription) o;

        return valueStartLocation == that.valueStartLocation;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + valueStartLocation;
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaLeafDescription{" +
                "valueStartLocation=" + valueStartLocation +
                '}';
    }
}
