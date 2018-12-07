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
import java.util.ArrayList;
import java.util.List;

public class JsonSchemaNonLeafDescription extends JsonSchemaDescription {

    private final List<JsonSchemaNameValue> inners = new ArrayList<JsonSchemaNameValue>();

    public JsonSchemaNonLeafDescription(JsonSchemaNonLeafDescription parent) {
        super(parent);
    }

    @Override
    public void writeTo(BufferObjectDataOutput out, int stringIndex, int descriptionIndex) throws IOException {
        int childCount = getChildCount();
        out.writeShort(childCount);
        int nextMetaDataAddress = out.position();
        int nextDescriptionAddres = nextMetaDataAddress + 8 * childCount;
        for (int i = 0; i < childCount; i++) {
            JsonSchemaNameValue nameValue = getChild(i);
            JsonSchemaDescription childValue = nameValue.getValue();
            out.position(nextMetaDataAddress);
            out.writeInt(nameValue.getNameStart() + stringIndex);
            out.writeInt(nextDescriptionAddres);
            nextMetaDataAddress = out.position();
            out.position(nextDescriptionAddres);
            childValue.writeTo(out, stringIndex, descriptionIndex);
            nextDescriptionAddres = out.position();
        }
    }

    public void addInner(JsonSchemaNameValue description) {
        inners.add(description);
    }

    public List<JsonSchemaNameValue> getInners() {
        return inners;
    }

    public JsonSchemaNameValue getChild(int i) {
        return inners.get(i);
    }

    public int getChildCount() {
        return inners.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        JsonSchemaNonLeafDescription that = (JsonSchemaNonLeafDescription) o;

        return inners != null ? inners.equals(that.inners) : that.inners == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (inners != null ? inners.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaNonLeafDescription{" +
                "inners=" + inners +
                '}';
    }
}
