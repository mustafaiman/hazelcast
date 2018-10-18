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
import com.hazelcast.internal.json.JsonValue;

import java.util.ArrayList;
import java.util.List;

public class LogicalJsonSearch {

    public static List<Integer> findColons(String text) {
        List<Integer> list = new ArrayList<Integer>(10);
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == ':') {
                list.add(i);
            }
        }
        return list;
    }

    private static int skipWhitespace(String text, int i) {
        i++;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == ' ' || c == '\n' || c == '\t') {
                i++;
            } else {
                break;
            }
        }
        return i;
    }

    private static JsonValue extractValue(String text, int colonLocation) {
        int start = skipWhitespace(text, colonLocation);
        int i;
        for (i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\n' || c == '\t' || c == ' ' || c == ',' || c == '}') {
                break;
            }
        }
        return Json.parse(text.substring(start, i));
    }

    public static boolean possiblyMatchesEqual(String text, String attributeName, String value, List<Integer> colonLocations) {
        int prevColon = 0;
        int currentColon;
        int nextColon;
        for (int i = 0; i < colonLocations.size() - 1; i++) {
            currentColon = colonLocations.get(i);
            nextColon = colonLocations.get(i+1);
            if (boyerMooreSearch(text, attributeName, prevColon, currentColon)
                    && boyerMooreSearch(text, value, currentColon, nextColon)) {
                return true;
            }
            prevColon = currentColon;
        }
        currentColon = colonLocations.get(colonLocations.size() - 1);
        nextColon = text.length();
        if (boyerMooreSearch(text, attributeName, prevColon, currentColon)
                && boyerMooreSearch(text, value, currentColon, nextColon)) {
            return true;
        }
        return false;
    }

    public static boolean possiblyMatchesGreaterLess(String text, String attributeName, Comparable givenValue, boolean equal, boolean less, List<Integer> colonLocations) {
//        for (Integer colonLoc: colonLocations) {
//            if (boyerMooreSearch(text, colonLoc, attributeName)) {
//                JsonValue value = extractValue(text, colonLoc);
//                if (value.isNumber()) {
//                    int comparison = givenValue.compareTo(value.asInt());
//                    if (equal && comparison == 0) {
//                        return true;
//                    } else if (less && comparison > 0) {
//                        return true;
//                    } else if (!less && comparison < 0) {
//                        return true;
//                    }
//                }
//            }
//        }
        return false;
    }

    public static boolean boyerMooreSearch(String text, String pattern, int start, int end) {
        int patternSize = pattern.length();

        int i = start, j = 0;

        while ((i + patternSize) <= end) {
            j = patternSize - 1;
            while (text.charAt(i + j) == pattern.charAt(j)) {
                j--;
                if (j < 0)
                    return true;
            }
            i++;
        }
        return false;
    }

//    public static boolean boyerMooreSearch(String text, String pattern, int start, int end) {
//        Pattern matcher = Pattern.compile(pattern, Pattern.LITERAL);
//        return matcher.matcher(text).region(start, end).find();
//    }

}
