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
import com.hazelcast.query.impl.QueryableEntry;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonEqualWithFilteringPredicate extends EqualPredicate {

    public JsonEqualWithFilteringPredicate() {
        super();
    }

    public JsonEqualWithFilteringPredicate(String attribute) {
        super(attribute);
    }

    public JsonEqualWithFilteringPredicate(String attribute, Comparable value) {
        super(attribute, value);
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        return super.filter(queryContext);
    }

    @Override
    public boolean apply(Map.Entry mapEntry) {
        String jsonString = (String) mapEntry.getValue();
        if (filterOut(jsonString)) {
            return false;
        }
        JsonObject object = Json.parse(jsonString).asObject();
        return object.get(attributeName).equals(JsonUtil.convertToJsonLiteral(value));
    }

    protected boolean filterOut(String jsonString) {
        List<Integer> colons = LogicalJsonSearch.findColons(jsonString);
        return !LogicalJsonSearch.possiblyMatchesEqual(jsonString, attributeName, value.toString(), colons);
    }

    @Override
    public boolean isIndexed(QueryContext queryContext) {
        return false;
    }


    @Override
    public int getId() {
        return PredicateDataSerializerHook.JSON_EQUAL_PREDICATE;
    }
}
