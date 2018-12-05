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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.BufferObjectDataInput;

import java.util.ArrayList;
import java.util.List;

public class ByteArrayLeveledPositionList implements LeveledColonPositionList {

    private static final int WORD_LEN = 64;

    private BufferObjectDataInput leveledColons;
    private int len;
    private int pos;

    public ByteArrayLeveledPositionList(BufferObjectDataInput buffer, int lengthInLongs, int pos) {
        this.leveledColons = buffer;
        this.len = lengthInLongs;
        this.pos = pos;
    }

    public List<Integer> getColons(int level, int start, int end) {
        try {
            List<Integer> posList = new ArrayList<Integer>();
            for (int i = start / WORD_LEN; i <= (((end - 1) / WORD_LEN) + 1); i++) {
                int charIndex = (level * len + i) * 8;
                long mcolon = leveledColons.readLong(pos + charIndex);
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
        } catch (Exception e) {
            throw new HazelcastException("sddf");
        }
    }

    @Override
    public int getLengthInLongs() {
        return len;
    }

    @Override
    public long[] getIndexArray() {
        return new long[0];
    }

    public BufferObjectDataInput getBuffer() {
        return leveledColons;
    }

    public void dispose() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArrayLeveledPositionList that = (ByteArrayLeveledPositionList) o;

        if (len != that.len) return false;
        return leveledColons != null ? leveledColons.equals(that.leveledColons) : that.leveledColons == null;
    }

    @Override
    public int hashCode() {
        int result = leveledColons != null ? leveledColons.hashCode() : 0;
        result = 31 * result + len;
        return result;
    }
}
