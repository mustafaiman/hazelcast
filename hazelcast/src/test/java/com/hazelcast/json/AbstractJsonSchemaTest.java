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

package com.hazelcast.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.io.IOException;
import java.nio.charset.Charset;

public abstract class AbstractJsonSchemaTest {

    private JsonFactory factory = new JsonFactory();

    protected JsonParser createParserFromString(String jsonString) throws IOException {
        byte[] bytes = jsonString.getBytes(Charset.forName("UTF8"));
        return factory.createParser(bytes, 0, bytes.length);
    }

    protected void printWithGuides(String str) {
        StringBuilder builder = new StringBuilder();
        int l = str.length();
        if (l > 10) {
            for (int i = 0; i < l; i++) {
                builder.append(i % 10 == 0 ? ((i / 10) % 10): " ");
            }
            builder.append('\n');
        }
        for (int i = 0; i < l; i++) {
            builder.append(i % 10);
        }
        builder.append('\n');
        builder.append(str);
        System.out.println(builder.toString());
    }
}
