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
import java.util.concurrent.ConcurrentLinkedQueue;

public class ByteBufferPool {

    private static int MIN_POOL_ITEM_LEN = 5 * 200;
    private static int POOL_SIZE = 1000;

    private ConcurrentLinkedQueue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<ByteBuffer>();

    public ByteBufferPool() {
        for (int i = 0; i < POOL_SIZE; i++) {
            bufferPool.add(ByteBuffer.allocateDirect(MIN_POOL_ITEM_LEN));
        }
    }

    public ByteBuffer allocateBuffer(int size) {
        ByteBuffer buffer = bufferPool.remove();
        if (buffer.limit() < size) {
            throw new RuntimeException("buffer is not enoguh size");
        }
        return buffer;
    }

    public void releaseBuffer(ByteBuffer buffer) {
        bufferPool.add(buffer);
    }
}
