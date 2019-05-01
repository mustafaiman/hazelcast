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

public class MapLoaderEntry<V> {

    private static final long NO_TIME_SET = Long.MIN_VALUE;

    private final V value;

    private final long expirationTime;

    public MapLoaderEntry(V value) {
        this.value = value;
        this.expirationTime = NO_TIME_SET;
    }

    public MapLoaderEntry(V value, long expirationTime) {
        this.value = value;
        this.expirationTime = expirationTime;
    }

    /**
     * Returns the value
     * @return
     */
    public V getValue() {
        return value;
    }

    /**
     * The expiration date of this entry. The entry is removed from
     * maps after the specified date. This value overrides any expiration
     * time calculated by using ttl and idle time configurations, both
     * per key and per map configurations.
     *
     * @return  the difference, measured in milliseconds, between
     *          the expiration time and midnight, January 1, 1970 UTC.
     */
    public long getExpirationTime() {
        return expirationTime;
    }
}
