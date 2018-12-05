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

import com.hazelcast.nio.BufferObjectDataInput;

import java.io.IOException;

public class DataStructuralIndex extends StructuralIndex {

    private BufferObjectDataInput data;
    private int offset;
    private int stringLen;


    public DataStructuralIndex(BufferObjectDataInput data, int offset, int stringLen, LeveledColonPositionList leveledIndex) {
        this.data = data;
        this.offset = offset;
        this.leveledIndex = leveledIndex;
        this.stringLen = stringLen;
    }

    @Override
    protected char charAt(int index) {
        try {
            char c = data.readChar(offset + index * 2);
            return c;
        } catch (IOException e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    protected int length() {
        return stringLen;
    }
}
