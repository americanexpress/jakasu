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

package com.americanexpress.jakasu.subscriber.examples.subscriber;

import com.americanexpress.jakasu.subscriber.exceptions.SubscriberException;
import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class TestSubscriber implements Subscriber {
    /**
     * Example Jakasu subscriber which only logs messages
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSubscriber.class);
    private int retryCount = 0; //count times the message is resent

    @Override
    public boolean recoveryHandler(String payLoad, Map<String, String> headers) {
        LOGGER.info("Inside recovery handler for {} {}", payLoad, headers);
        return true;
    }

    @Override
    public void handleError(SubscriberException ex) {
        LOGGER.error("Error occured while processing message: {}", ex);
    }

    @Override
    public boolean subscribe(String payLoad, Map<String, String> headers) {
        //test the message is resent when return false
        //set a counter so the message isn't resent infinitely
        if (payLoad.contains("retry") && retryCount < 5) {
            LOGGER.info("Test subscriber received {} {} time(s)", payLoad, ++retryCount);
            return false;
        } else {
            LOGGER.info("Input topic Subscriber received: {} {}", payLoad, headers);
            return true;
        }
    }
}
