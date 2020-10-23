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

import com.americanexpress.jakasu.subscriber.helpers.Util;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeduplicationTransformer.class);


    private static final String STORE_NAME = "eventId-store";
    /* internal topic is named after consumer group id + eventId-store
     * therefore a msg processed by a different consumer group on the same
     * topic is not a duplicate when seen by this consumer group
     */
    private ProcessorContext context;

    private WindowStore<String, Long> eventIdStore;

    private long leftDurationMs;
    private long rightDurationMs;
    private String dedupHeaderKey;

    /**
     * Streams transformer to check whether this message is a duplicate of another msg
     * If so, message is skipped
     * This transformer is added to streams topology if dedup is enabled
     *
     * @param maintainDurationPerEventInMs rolling window of age of events to compare against
     * @param dedupHeaderKey               key of a header which user knows will be included
     *                                     in messages which can be used as a unique identifier
     *                                     for each message, ideally an id generated from
     *                                     the source of the event
     */
    public DeduplicationTransformer(final long maintainDurationPerEventInMs, String dedupHeaderKey) {
        if (maintainDurationPerEventInMs < 1) {
            throw new IllegalArgumentException("maintain duration per event must be >= 1");
        }
        leftDurationMs = maintainDurationPerEventInMs / 2;
        rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
        this.dedupHeaderKey = dedupHeaderKey;
    }

    /**
     * Get store of events in internal dedup topic
     *
     * @param processorContext get metadata for this streams processor
     */
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        eventIdStore = (WindowStore<String, Long>) context.getStateStore(STORE_NAME);
    }

    /**
     * Returns original message if it is unique, else null
     *
     * @param key   message key
     * @param value message value
     * @return original header or null
     */
    @Override
    public KeyValue<K, V> transform(K key, V value) {
        String eventId = generateId();

        if (eventId == null) {
            LOGGER.error("Message did not contain header {} and cannot be checked for deduplication.", dedupHeaderKey);
        } else if (isDuplicate(eventId)) {
            LOGGER.info("Duplicate event reported for message {} {}. Message will be skipped.", key, value);
            updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
            return null;
        } else {
            LOGGER.debug("Original event reported for message {} {}", key, value);
            rememberNewEvent(eventId, context.timestamp());
        }

        return KeyValue.pair(key, value);
    }

    /**
     * Get the value of the header which has been designated as a unique id
     *
     * @return id header value
     */
    private String generateId() {
        return Util.INSTANCE.getheadersAsMap(context.headers()).get(dedupHeaderKey);
    }

    /**
     * Fetch from the message store of the internal dedup topic
     * messages which match the ID of this event
     * and occurred within the time interval
     *
     * @param eventId id of this event
     * @return true if any such duplicate event exists
     */
    private boolean isDuplicate(String eventId) {
        final long eventTime = context.timestamp();

        final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                eventId,
                eventTime - leftDurationMs,
                eventTime + rightDurationMs);
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

    /**
     * Update the timestamp of the event to keep it from expiring
     *
     * @param eventId      id
     * @param newTimestamp new time
     */
    private void updateTimestampOfExistingEventToPreventExpiry(String eventId, final long newTimestamp) {
        eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    /**
     * Save a unique event in the store to be remember for later comparisons
     *
     * @param eventId   id
     * @param timestamp event time
     */
    private void rememberNewEvent(String eventId, final long timestamp) {
        eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {
        //nothing to shutdown
    }
}
