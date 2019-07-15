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

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.io.UTFDataFormatException;

class Utf8Reader extends Reader {

    private final DataInput input;
    private byte leapByte = -1;

    Utf8Reader(DataInput input) {
        this.input = input;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int maxChars = cbuf.length - off;
        int i = 0;
        try {
            for (i = 0; i < len && i < maxChars; i++) {
                byte firstByte = leapByte;
                if (firstByte == -1) {
                    firstByte = input.readByte();
                } else {
                    leapByte = -1;
                }

                int b = firstByte & 0xFF;
                switch (b >> 4) {
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                    case 5:
                    case 6:
                    case 7:
                        cbuf[off + i] = (char) b;
                        break;
                    case 12: // 110x xxxx - 10xx xxxx
                    case 13:
                        int first = (b & 0x1F) << 6;
                        int second = input.readByte() & 0x3F;
                        cbuf[off + i] = (char) (first | second);
                        break;
                    case 14: // 1110 xxxx - 10xx xxxx - 10xx xxxx
                        int first2 = (b & 0x0F) << 12;
                        int second2 = (input.readByte() & 0x3F) << 6;
                        int third2 = input.readByte() & 0x3F;
                        cbuf[off + i] = (char) (first2 | second2 | third2);
                        break;
                    case 15: // 1111 0xxx - 10xx xxxx - 10xx xxxx - 10xx xxxx
                        if (i + 1 < maxChars) {
                            int first3 = (b & 0x7) << 18;
                            int second3 = (input.readByte() & 0x3F) << 12;
                            int third3 = (input.readByte() & 0x3F) << 6;
                            int fourth3 = input.readByte() & 0x3F;
                            int unicode = (first3 | second3 | third3 | fourth3) - 0x10000;
                            cbuf[off + i] = (char) (0xD800 | (unicode >> 10));
                            i++;
                            cbuf[off + i] = (char) (0xDC00 | (unicode & 0x3FF));
                        } else {
                            leapByte = firstByte;
                            return i;
                        }
                        break;
                    default:
                        throw new UTFDataFormatException("Malformed byte sequence");
                }
            }
        } catch (EOFException e) {
            if (i == 0) {
                return -1;
            }
        }
        return i;
    }

    @Override
    public void close() throws IOException {

    }
}
