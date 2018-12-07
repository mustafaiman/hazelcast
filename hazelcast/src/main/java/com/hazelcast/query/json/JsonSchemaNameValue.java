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

public class JsonSchemaNameValue {

    private final int nameStart;
    private final JsonSchemaDescription value;

    public JsonSchemaNameValue(int nameStart, JsonSchemaDescription value) {
        this.nameStart = nameStart;
        this.value = value;
    }

    public int getNameStart() {
        return nameStart;
    }

    public JsonSchemaDescription getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JsonSchemaNameValue nameValue = (JsonSchemaNameValue) o;

        if (nameStart != nameValue.nameStart) return false;
        return value != null ? value.equals(nameValue.value) : nameValue.value == null;
    }

    @Override
    public int hashCode() {
        int result = nameStart;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JsonSchemaNameValue{" +
                "nameStart=" + nameStart +
                ", value=" + value +
                '}';
    }
}
