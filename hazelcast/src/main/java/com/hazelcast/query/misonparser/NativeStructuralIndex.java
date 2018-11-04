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

import com.hazelcast.internal.json.JsonValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class NativeStructuralIndex extends StructuralIndex {

    protected ByteBufferPool allocator;

    public NativeStructuralIndex(String sequence, int maxNesting, ByteBufferPool allocator) {
        this.sequence = sequence;
        this.allocator = allocator;
        this.maxNesting = maxNesting;
        createLeveledIndex();
    }

    @Override
    protected void createLeveledIndex() {
        int indexLength = (((sequence.length() - 1)  >> 7) + 1) << 1;
        ByteBuffer buf = allocator.allocateBuffer(indexLength * 7);
        SIMDHelper.createCharacterIndexes(sequence, maxNesting, buf);

        leveledIndex = new BufferLeveledColonPositionList(buf, indexLength);
    }

    @Override
    public JsonValue findValueByPattern(List<Integer> pattern, String[] parts) {
        int start = 0;
        int end = sequence.length();
        int level = 1;
        int colonLoc = -1;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            List<Integer> colonLocations = leveledIndex.getColons(level, start, end);
            int colonIndex = pattern.get(i);
            if (colonLocations.size() <= colonIndex) {
                return null;
            }
            colonLoc = colonLocations.get(colonIndex);
            if (attributeNameMatches(colonLoc, part)) {
                start = colonLoc + 1;
                if (colonIndex + 1 < colonLocations.size()) {
                    end = colonLocations.get(colonIndex+1);
                }
                level++;
            } else {
                return null;
            }
        }
        return readJsonValue(colonLoc);
    }

    @Override
    public List<Integer> findPattern(String[] parts) {
        ArrayList<Integer> pattern = new ArrayList<Integer>();
        int start = 0;
        int end = sequence.length();
        int level = 1;
        for (String part: parts) {
            List<Integer> colonLocations = leveledIndex.getColons(level, start, end);
            boolean found = false;
            for (int i = 0; i < colonLocations.size(); i++) {
                int colonLoc = colonLocations.get(i);
                if (attributeNameMatches(colonLoc, part)) {
                    start = colonLoc + 1;
                    if (i + 1 < colonLocations.size() ) {
                        end = colonLocations.get(i+1);
                    }
                    level++;
                    found = true;
                    pattern.add(i);
                    break;
                }
            }
            if (!found) {
                return null;
            }
        }
        return pattern;
    }

    @Override
    public void dispose() {
        allocator.releaseBuffer(((BufferLeveledColonPositionList)leveledIndex).getBuffer());
    }

}
