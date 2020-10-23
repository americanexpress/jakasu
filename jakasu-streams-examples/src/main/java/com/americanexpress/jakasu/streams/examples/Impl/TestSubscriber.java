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

package com.americanexpress.jakasu.streams.examples.Impl;

import com.americanexpress.jakasu.subscriber.exceptions.SubscriberException;
import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Sample test Subscriber implementation which logs messages
 * <p>
 * Demonstrates how a standard Jakasu Subscriber can be used in parallel
 * with a streams subscriber.
 * Subscribed to output topic of streams processor to verify that
 * modifications were successful.
 */
@Component
public class TestSubscriber implements Subscriber {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSubscriber.class);


    @Override
    public boolean recoveryHandler(String payLoad, Map<String, String> headers) {
        LOGGER.info(payLoad);
        return true;
    }

    @Override
    public void handleError(SubscriberException ex) {
        LOGGER.error("Error occured while processing message: ");
    }

    @Override
    public boolean subscribe(String payLoad, Map<String, String> headers) throws SubscriberException {
        LOGGER.info("Standard Subscriber received: {} {}", payLoad, headers);
        return true;
    }
}
