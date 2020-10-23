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

package com.americanexpress.jakasu.subscriber.subscribe;

import com.americanexpress.jakasu.subscriber.exceptions.SubscriberException;

import java.util.Map;

public interface Subscriber {
    /* IMPORTANT - Jakasu SDK users must implement this interface with logic of how
     * each Kafka message should be processed and how to react
     * to failures while processing
     */

    /**
     * Implement this method to process each message
     *
     * @param payload - message body (payload) of the message consumed from the Kafka topic
     * @param headers - header of the message consumed
     * @return true - message processing was successful
     * false - message should be sent again to be reprocessed
     * if it is necessary that messages be processed atomically,
     * or failure was due to an issue which will likely be resolved
     * (ex. lost connection with a downstream system)
     * @throws SubscriberException - In the implementation, any known exception to the implementer
     *                             should be rethrown as a Subscriber exception to trigger error handler then continue to the next message
     */
    boolean subscribe(String payload, Map<String, String> headers) throws SubscriberException;

    /**
     * Message Retry is triggered only when an unexpected / runtime exception occurs
     * When all retry gets exhausted, just before exiting the kafka listener, this method
     * provides alternate way to process the message using recovery. If the alternate path of processing
     * works then, the consumer will use this path to process this message and continue with
     * newer consumption of messages.
     * If this alternate path fails, then the framework logs the error and
     * safely shuts down the kafka listeners.
     *
     * @param payLoad - message payload or the message which you consume from the kafka topic
     * @param headers - header of the message which was consume
     * @return true/false success
     */
    boolean recoveryHandler(String payLoad, Map<String, String> headers);

    /**
     * This method is invoked by the Jakasu framework when the subscribe method
     * throws a SubscriberException()
     * User can log / handle this error, and the offset is acknowledged
     * to continue processing with the next message in the topic
     *
     * @param ex - the subscriber exception thrown from subscribe method
     */
    void handleError(SubscriberException ex);
}
