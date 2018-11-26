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

import java.util.ArrayList;
import java.util.List;

public class ArrayLeveledColonPositionsList implements LeveledColonPositionList {
    private static final int WORD_LEN = 64;

    private long[] leveledColons;
    private int len;

    public ArrayLeveledColonPositionsList(long[] buffer, int lengthInLongs) {
        this.leveledColons = buffer;
        this.len = lengthInLongs;
    }

    public List<Integer> getColons(int level, int start, int end) {
        List<Integer> posList = new ArrayList<Integer>(10);
        for (int i = start/WORD_LEN; i < (((end - 1) / WORD_LEN) + 1); i++) {
            int charIndex = level * len + i;
            long mcolon = leveledColons[charIndex];
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
        return leveledColons;
    }
}
