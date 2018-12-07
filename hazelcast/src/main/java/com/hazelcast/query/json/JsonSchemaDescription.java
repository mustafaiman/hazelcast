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

public abstract class JsonSchemaDescription {

    private JsonSchemaNonLeafDescription parent;

    public JsonSchemaDescription(JsonSchemaNonLeafDescription parent) {
        this.parent = parent;
    }

    public JsonSchemaNonLeafDescription getParent() {
        return parent;
    }

    public abstract void writeTo(BufferObjectDataOutput out, int stringIndex, int descriptionIndex) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JsonSchemaDescription that = (JsonSchemaDescription) o;

        return parent != null ? parent.equals(that.parent) : that.parent == null;
    }

    @Override
    public int hashCode() {
        return parent != null ? parent.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "JsonSchemaDescription{" +
                "parent=" + parent +
                '}';
    }
}
