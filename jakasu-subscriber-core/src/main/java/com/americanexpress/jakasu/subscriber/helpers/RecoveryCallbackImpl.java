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

package com.americanexpress.jakasu.subscriber.helpers;

import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;

import java.util.Map;

public class RecoveryCallbackImpl implements RecoveryCallback<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryCallbackImpl.class);

    private Subscriber subscriber;
    private ConcurrentMessageListenerContainer container;


    public RecoveryCallbackImpl(Subscriber subscriber, ConcurrentMessageListenerContainer container) {
        this.subscriber = subscriber;
        this.container = container;
    }

    /**
     * KafkaListener container factory wraps the listeners in a retrying adapter,
     * which uses this RecoveryCallback to try processing the failed message again
     * This recover method passes the message to the user's recoveryHandler method in
     * their Subscriber implementation.
     * If there are more exceptions during recovery, the listener container is shut down,
     * and no more messages will be processed by this subscriber instance.
     *
     * @param retryContext retry context
     * @return void
     */
    @Override
    @SuppressWarnings("unchecked")
    public Void recover(RetryContext retryContext) {
        ConsumerRecord<String, String> record = (ConsumerRecord<String, String>) retryContext.getAttribute("record");
        try {
            Map<String, String> headers = Util.INSTANCE.getheadersAsMap(record.headers());
            subscriber.recoveryHandler(record.value(), headers);
            Acknowledgment acknowledgment = (Acknowledgment) retryContext.getAttribute("acknowledgment");

            acknowledgment.acknowledge();
        } catch (Exception ex) {
            LOGGER.error("Failed in recovery Callback: {}", ex);
            container.stop();
        }
        return null;
    }

}