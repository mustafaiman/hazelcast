/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EntryStoreImpl implements EntryStore<String, String> {

    private Map<String, String> keyValueTable;

    private Map<String, Long> keyExpirationTimeTable;

    @Override
    public void store(String key, MapLoaderEntry<String> value) {
        long expTime = value.getExpirationTime();
        String val = value.getValue();
        keyValueTable.put(key, val);
        keyExpirationTimeTable.put(key, expTime);
    }

    @Override
    public void storeAll(Map<String, MapLoaderEntry<String>> map) {
        for (Map.Entry<String, MapLoaderEntry<String>> entry: map.entrySet()) {
            long expTime = entry.getValue().getExpirationTime();
            String val = entry.getValue().getValue();
            String key = entry.getKey();

            keyValueTable.put(key, val);
            keyExpirationTimeTable.put(key, expTime);
        }
    }

    @Override
    public void delete(String key) {
        keyValueTable.remove(key);
        keyExpirationTimeTable.remove(key);
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        for (String key: keys) {
            keyValueTable.remove(key);
            keyExpirationTimeTable.remove(key);
        }
    }

    @Override
    public MapLoaderEntry<String> load(String key) {
        String val = keyValueTable.get(key);
        if (val == null) {
            return null;
        }
        long expTime = keyExpirationTimeTable.get(key);

        return new MapLoaderEntry<>(val, expTime);
    }

    @Override
    public Map<String, MapLoaderEntry<String>> loadAll(Collection<String> keys) {
        Map<String, MapLoaderEntry<String>> map = new HashMap<>();
        for (String key: keys) {
            String val = keyValueTable.get(key);
            long expTime = keyExpirationTimeTable.get(key);
            map.put(key, new MapLoaderEntry<>(val, expTime));
        }
        return map;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        return keyValueTable.keySet();
    }
}
