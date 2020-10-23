/*
 *
 * Copyright 2020 American Express Travel Related Services Company, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.americanexpress.jakasu.subscriber.streams;

import com.americanexpress.jakasu.subscriber.filter.Filter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilterTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTransformer.class);

    private ProcessorContext context;
    private Filter filter;

    /**
     * Transformer to wrap Filter
     * If user config enables filtering, this processor is
     * added to the streams topology which will only pass along
     * messages which match the filter key value pairs specified
     *
     * @param filter to apply
     */
    public FilterTransformer(Filter filter) {
        this.filter = filter;
    }

    /**
     * Get context for headers
     *
     * @param processorContext context
     */
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    /**
     * Only return message which matches filter
     *
     * @param key   header key of message
     * @param value header val of message
     * @return Original message to pass along or null if filtered out
     */
    @Override
    public KeyValue<K, V> transform(K key, V value) {
        // convert to consumer record so that user need not specify a different filter strategy
        // for both streams and regular consumer
        // checksum is unneeded since this record will only live during the filtering
        ConsumerRecord record = new ConsumerRecord(context.topic(), context.partition(), context.offset(),
                context.timestamp(), TimestampType.NO_TIMESTAMP_TYPE, null, ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE, key, value, context.headers());
        if (filter.filter(record) == null) {
            LOGGER.debug("Message with key '{}', value '{}' was filtered out", key, value);
            return null;
        }
        LOGGER.debug("Message with key '{}', value '{}' was retained by filter", key, value);
        return KeyValue.pair(key, value);
    }

    @Override
    public void close() {
        //nothing to shut down
    }
}
