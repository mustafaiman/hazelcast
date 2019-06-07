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

package com.hazelcast.core;

/**
 * This exception is thrown when Hazelcast operations encounter problems
 * while interacting external systems such as file systems, custom code,
 * etc. .
 */
public class HazelcastExternalException extends HazelcastException {
    public HazelcastExternalException(Throwable cause) {
        super(cause);
    }

    public HazelcastExternalException(String message) {
        super(message);
    }

    public HazelcastExternalException(String message, Throwable cause) {
        super(message, cause);
    }
}
