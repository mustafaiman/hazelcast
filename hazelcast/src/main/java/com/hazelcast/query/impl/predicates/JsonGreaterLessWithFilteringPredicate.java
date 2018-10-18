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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.query.impl.QueryContext;

import java.util.List;
import java.util.Map;

public class JsonGreaterLessWithFilteringPredicate extends GreaterLessPredicate {

    public JsonGreaterLessWithFilteringPredicate() {
        super();
    }

    public JsonGreaterLessWithFilteringPredicate(String attribute, Comparable value, boolean equal, boolean less) {
        super(attribute, value, equal, less);
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        String jsonString = (String) mapEntry.getValue();
        if (filterOut(jsonString)) {
            return false;
        }
        JsonObject object = Json.parse(jsonString).asObject();
        int result = ((Comparable)object.get(attributeName).asInt()).compareTo(value);
        return equal && result == 0 || (less ? (result < 0) : (result > 0));
    }

    protected boolean filterOut(String jsonString) {
        List<Integer> colons = LogicalJsonSearch.findColons(jsonString);
        if (!LogicalJsonSearch.possiblyMatchesGreaterLess(jsonString, attributeName, value, equal, less, colons)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }

    @Override
    public int getId() {
        return PredicateDataSerializerHook.JSON_GREATER_LESS_PREDICATE;
    }
}
