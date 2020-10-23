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

package com.americanexpress.jakasu.subscriber.services;

import com.americanexpress.jakasu.subscriber.exceptions.ServiceException;
import com.americanexpress.jakasu.subscriber.exceptions.SubscriberException;
import com.americanexpress.jakasu.subscriber.helpers.Util;
import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Map;

public class SubscriberService {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(SubscriberService.class);


    Subscriber subscriber;

    public SubscriberService(Subscriber subscriber) {
        this.subscriber = subscriber;
    }

    /**
     * Subscriber service is set as the Spring Kafka message Listener.
     * Each message for a consumer group is sent to this method,
     * which hands it to the user's Subscriber, handles any errors,
     * then acknowledges that the message was successfully consumed.
     * Once a message is acknowledged, the next is sent for processing
     * until all are consumed.
     *
     * @param record Kafka record
     * @param ack    to acknowledge to broker that message processed
     * @return message processed?
     */
    public void serve(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("Subscriber service received message: {} with headers: {} from topic: {} serving to: {}",
                        record.value(),
                        Util.INSTANCE.headersBytesToString(record.headers()),
                        record.topic(),
                        subscriber
                );

            Map<String, String> headers = Util.INSTANCE.getheadersAsMap(record.headers());

            // if there was some problem with message which does not necessitate an exception,
            // Subscriber can return false to skip acknowledgement and reprocess the message
            if (!subscriber.subscribe(record.value(), headers) && LOGGER.isDebugEnabled()) {
                LOGGER.debug("Subscriber returned false from message processing but did not report an error for message {} with headers {}. Message will be reprocessed", record.value(), record.headers());
                ack.nack(0);
            }
            ack.acknowledge();
        } catch (SubscriberException subEx) {
            LOGGER.error("Subscription exception was reported for message at topic - {} at the offset - {} ", record.topic(), record.offset());
            subscriber.handleError(subEx);
            ack.acknowledge();
        } catch (Exception ex) {
            throw new ServiceException(String.format("Exception occurred during servicing record with offset: %d and value: %s", record.offset(), record.value()), ex);
        }
    }
}
