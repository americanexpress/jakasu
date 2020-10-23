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
import com.google.gson.JsonArray;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatmapTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlatmapTransformer.class);

    private ProcessorContext context;

    private String idHeaderKey;

    /**
     * Streams transformer to split a bulk event in JSON array format into
     * events individually forwarded to output topic with their own unique
     * ids
     *
     * @param flatmapHeaderKey key of a header which user knows will be included
     *                         in messages which can be used as a unique identifier
     *                         for each message, ideally an id generated from
     *                         the source of the event
     */
    public FlatmapTransformer(String flatmapHeaderKey) {
        this.idHeaderKey = flatmapHeaderKey;
    }

    /**
     * Get context for headers
     *
     * @param processorContext get metadata for this streams processor
     */
    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    /**
     * Splits a bulk event in JSON array format into
     * individual events
     *
     * @param key   message key
     * @param value message value
     * @return null if bulk event successfully split, else unmodified payload
     */
    @Override
    public KeyValue<K, V> transform(K key, V value) {
        String eventId = getId();
        if (eventId == null) {
            LOGGER.warn("Message did not contain header '{}'", idHeaderKey);
        }

        LOGGER.debug("Enabling flatmapstream on message with id: {}", eventId);

        JsonArray events = null;
        try {
            events = JsonParser.parseString(value.toString()).getAsJsonArray();
        } catch (JsonParseException e) {
            LOGGER.warn("Message is not in JSON array format and cannot be split", idHeaderKey);
            return new KeyValue<>(key, value); // return message as is
        }

        // forward each event
        for (int i = 0; i < events.size(); i++) {
            if (eventId != null) setIdHeader(eventId, i);
            context.forward(key, events.get(i).toString());
        }

        return null; // null - all events already forwarded
    }

    /**
     * Get the value of the header which has been designated as a unique id
     *
     * @return id header value
     */
    private String getId() {
        return Util.INSTANCE.getheadersAsMap(context.headers()).get(idHeaderKey);
    }

    /**
     * Appends index to event id for a message so
     * each event of a bulk event has a unique id
     * ex. bulk event w/ 3 events
     * bulk id = abc
     * event 0 id = abc-0
     * event 1 id = abc-1
     * event 2 id = abc-2
     *
     * @param eventId    bulk event id
     * @param eventIndex this event's position in bulk event array
     */
    private void setIdHeader(String eventId, int eventIndex) {
        context.headers().remove(idHeaderKey);
        context.headers().add(idHeaderKey, (eventId + "-" + eventIndex).getBytes());
    }

    @Override
    public void close() {
        //nothing to shutdown
    }
}

