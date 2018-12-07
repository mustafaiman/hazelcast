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

import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.internal.json.Json;
import com.hazelcast.query.json.JsonSchemaCreator;
import com.hazelcast.query.json.JsonSchemaDescription;
import com.hazelcast.query.json.JsonSchemaLeafDescription;
import com.hazelcast.query.json.JsonSchemaNameValue;
import com.hazelcast.query.json.JsonSchemaNonLeafDescription;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category({ParallelTest.class, QuickTest.class})
@RunWith(HazelcastParallelClassRunner.class)
public class JsonSchemaCreatorTest extends AbstractJsonSchemaTest {


    @Test
    public void testOneFirstLevelAttribute() throws IOException {
        String jsonString = Json.object().add("name", "aName").toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0", 1, 8);
    }

    @Test
    public void testOneFirstLevelAttribute_withTwoByteCharacterInName() throws IOException {
        String jsonString = Json.object().add("this-name-includes-two-byte-utf8-character-£",
                "so the value should start at next byte location").toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0", 1, 49);
    }

    @Test
    public void testTwoFirstLevelAttributes() throws IOException {
        String jsonString = Json.object().add("name", "aName").add("age", 4).toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0", 1, 8);
        validate(description, "1", 16, 22);
    }

    @Test
    public void testThreeFirstLevelAttributes() throws IOException {
        String jsonString = Json.object()
                .add("name", "aName")
                .add("age", 4).add("location", "ankara")
                .toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0", 1, 8);
        validate(description, "1", 16, 22);
        validate(description, "2", 24, 35);
    }

    @Test
    public void testOneFirstLevelTwoInnerAttributes() throws IOException {
        String jsonString = Json.object()
                .add("name", Json.object()
                        .add("firstName", "fName")
                        .add("surname", "sname")
                )
                .toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0.0", 9, 21);
        validate(description, "0.1", 29, 39);
    }

    @Test
    public void testTwoFirstLevelOneInnerAttributesEach() throws IOException {
        String jsonString = Json.object()
                .add("name", Json.object()
                        .add("firstName", "fName"))
                .add("address", Json.object()
                        .add("addressId", 4))
                .toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0.0", 9,21);
        validate(description, "1.0", 41,53);
    }

    @Test
    public void testFourNestedLevels() throws IOException {
        String jsonString = Json.object()
                .add("firstObject", Json.object()
                        .add("secondObject", Json.object()
                                .add("thirdObject", Json.object()
                                        .add("fourthObject", true)))).toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "0.0.0.0", 47, 62);
    }

    @Test
    public void testFirstLevelValue() throws IOException {
        String jsonString = Json.value("name").toString();
        printWithGuides(jsonString);
        JsonParser parser = createParserFromString(jsonString);
        JsonSchemaNonLeafDescription description = (JsonSchemaNonLeafDescription) JsonSchemaCreator.createDescription(parser);

        validate(description, "this", -1, 0);
    }

    protected void validate(JsonSchemaNonLeafDescription root, String attributePath, int expectedNameLoc, int expectedValueLoc) {
        assertNotNull(root);
        assertNull(root.getParent());
        if (attributePath.equals("this")) {
            JsonSchemaNameValue nameValue = root.getInners().get(0);
            assertEquals(expectedNameLoc, nameValue.getNameStart());
            JsonSchemaLeafDescription leaf = (JsonSchemaLeafDescription) nameValue.getValue();
            assertEquals(expectedValueLoc, leaf.getValueStartLocation());
        } else {
            String[] path = attributePath.split("\\.");

            JsonSchemaNameValue nameValue = root.getInners().get(0);
            int nameLoc = nameValue.getNameStart();
            assertEquals(-1, nameLoc);
            JsonSchemaDescription object = nameValue.getValue();
            assertNotNull(object);
            for (String p : path) {
                nameValue = ((JsonSchemaNonLeafDescription) object).getInners().get(c(p));
                nameLoc = nameValue.getNameStart();
                object = nameValue.getValue();
            }
            assertEquals(String.format("\nExpected name location: %d\nActual name location: %d\n", expectedNameLoc, nameLoc), expectedNameLoc, nameLoc);
            int actualValueLoc = ((JsonSchemaLeafDescription) object).getValueStartLocation();
            assertEquals(String.format("\nExpected value location: %d\nActual value location: %d\n", expectedValueLoc, actualValueLoc), expectedValueLoc, actualValueLoc);
        }
    }

    private int c(String num) {
        return Integer.parseInt(num);
    }
}
