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

package com.hazelcast.internal.serialization.impl;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.junit.Assert.assertEquals;

public class Utf8ReaderTest {

    @Test
    public void testReadAscii() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("abcdefghij".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[10];
        assertRead("abcdefghij".toCharArray(), 10, reader, characters, 0, 10);
    }

    @Test
    public void testReadAscii_oneChar() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("a".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[1];
        assertRead("a".toCharArray(), 1, reader, characters, 0, 10);
    }

    @Test
    public void testReadAscii_noCharacterLeft() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("a".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[1];
        assertRead("a".toCharArray(), 1, reader, characters, 0, 10);
        assertNoRead(reader, characters, 0, 10);
    }

    @Test
    public void testReadAscii_whenSmallArray() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("abcdefghij".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[4];
        assertRead("abcd".toCharArray(), 4, reader, characters, 0, 4);
    }

    @Test
    public void testReadUtf8() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("şçiü emoji: \uD83D\uDE2D".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[14];
        assertRead("şçiü emoji: \uD83D\uDE2D".toCharArray(), 14, reader, characters, 0, 14);
    }

    @Test
    public void testReadUtf8_startingWithSurrogatePair() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("\uD83D\uDE2D".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[14];
        assertRead("\uD83D\uDE2D".toCharArray(), 2, reader, characters, 0, 2);
    }

    @Test
    public void testReadUtf8_doesNotRead4ByteCharacterIfNoSpace() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("şçiü emoji: \uD83D\uDE2D".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[13];
        assertRead("şçiü emoji: ".toCharArray(), 12, reader, characters, 0, 14);
    }

    @Test
    public void testReadUtf8_continuesReadingFromWhereItLeft_inCaseOfSurrogates() throws IOException {
        Utf8Reader reader = createUtf8Reader(new ByteArrayInputStream("şçiü emoji: \uD83D\uDE2D".getBytes(Charset.forName("UTF8"))));

        char[] characters = new char[13];
        assertRead("şçiü emoji: ".toCharArray(), 12, reader, characters, 0, 14);
        assertRead("\uD83D\uDE2D".toCharArray(), 2, reader, characters, 0, 14);
    }

    private void assertRead(char[] expectedChars, int expectedReadChars, Utf8Reader reader, char[] outputArray, int offset, int len) throws IOException {
        assertEquals(expectedReadChars, reader.read(outputArray, offset, len));
        for (int i = 0; i < expectedChars.length; i++) {
            assertEquals(expectedChars[i], outputArray[offset + i]);
        }
    }

    private void assertNoRead(Utf8Reader reader, char[] outputArray, int offset, int len) throws IOException {
        assertEquals(-1, reader.read(outputArray, offset, len));
    }


    private Utf8Reader createUtf8Reader(InputStream inputStream) {
        return new Utf8Reader(new DataInputStream(inputStream));
    }
}
