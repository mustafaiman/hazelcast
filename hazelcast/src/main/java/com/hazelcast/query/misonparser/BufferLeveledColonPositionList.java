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

package com.hazelcast.query.misonparser;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class BufferLeveledColonPositionList implements LeveledColonPositionList {

    private static final int WORD_LEN = 64;

    private ByteBuffer leveledColons;
    private int len;
    private ByteOrder byteOrder = ByteOrder.LITTLE_ENDIAN;

    public BufferLeveledColonPositionList(ByteBuffer buffer, int lengthInLongs) {
        this.leveledColons = buffer;
        this.len = lengthInLongs;
    }

    public BufferLeveledColonPositionList(ByteBuffer buffer, int lengthInLongs, ByteOrder byteOrder) {
        this(buffer, lengthInLongs);
        this.byteOrder = byteOrder;
    }

    public List<Integer> getColons(int level, int start, int end) {
        List<Integer> posList = new ArrayList<Integer>();
        leveledColons.order(byteOrder);
        for (int i = start/WORD_LEN; i <= (((end - 1) / WORD_LEN) + 1); i++) {
            int charIndex = (level * len +i ) * 8;
            long mcolon = leveledColons.getLong(charIndex);
            while (mcolon != 0) {
                long mbit = BitOperations.extractFirstSetBit(mcolon);
                int pad = Long.numberOfTrailingZeros(mbit);
                int offset = i * WORD_LEN + pad;
                if (start <= offset && offset < end) {
                    posList.add(offset);
                }
                mcolon = BitOperations.removeFirstSetBit(mcolon);
            }
        }
        return posList;
    }

    @Override
    public int getLengthInLongs() {
        return len;
    }

    @Override
    public long[] getIndexArray() {
        return new long[0];
    }

    public ByteBuffer getBuffer() {
        return leveledColons;
    }
}
