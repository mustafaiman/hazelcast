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

package com.hazelcast.map.impl.record;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.json.JsonSchemaCreator;
import com.hazelcast.query.json.JsonSchemaNonLeafDescription;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.HeapData.DATA_OFFSET;

public class DataRecordFactory implements RecordFactory<Data> {

    JsonFactory jsonFactory = new JsonFactory();

    private final SerializationService serializationService;
    private final PartitioningStrategy partitionStrategy;
    private final CacheDeserializedValues cacheDeserializedValues;
    private final boolean statisticsEnabled;

    public DataRecordFactory(MapConfig config, SerializationService serializationService,
                             PartitioningStrategy partitionStrategy) {
        this.serializationService = serializationService;
        this.partitionStrategy = partitionStrategy;
        this.statisticsEnabled = config.isStatisticsEnabled();
        this.cacheDeserializedValues = config.getCacheDeserializedValues();
    }

    @Override
    public Record<Data> newRecord(Object value) {
        assert value != null : "value can not be null";

        final Data data = serializationService.toData(value, partitionStrategy);
        AbstractRecord<Data> record;
        switch (cacheDeserializedValues) {
            case NEVER:
                record = statisticsEnabled ? new DataRecordWithStats(data) : new DataRecord(data);
                break;
            default:
                record = statisticsEnabled ? new CachedDataRecordWithStats(data) : new CachedDataRecord(data);
        }

        if (data.getType() == SerializationConstants.JAVA_JSONWITHMETADATA_INDEX) {
            try {
                JsonParser parser = jsonFactory.createParser(data.toByteArray(), 4 + DATA_OFFSET, data.dataSize() - 4);
                JsonSchemaNonLeafDescription description = JsonSchemaCreator.createDescription(parser);
                record.setQueryingData(description);
            } catch (IOException e) {
                throw new HazelcastException(e);
            }
        }
        return record;
    }

    @Override
    public void setValue(Record<Data> record, Object value) {
        assert value != null : "value can not be null";

        final Data v;
        if (value instanceof Data) {
            v = (Data) value;
        } else {
            v = serializationService.toData(value, partitionStrategy);
        }
        record.setValue(v);
    }
}
