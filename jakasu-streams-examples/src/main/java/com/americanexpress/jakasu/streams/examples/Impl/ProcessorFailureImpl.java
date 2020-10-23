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
import com.americanexpress.jakasu.subscriber.streams.StreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * A processor to demonstrate the capability of applying different processor
 * implementations and how errors are handled by the handleError method
 * when an exception occurs
 */
@Component
public class ProcessorFailureImpl extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorFailureImpl.class);

    @Override
    public String process(String payLoad, Map<String, String> headers) throws SubscriberException {
        throw new SubscriberException("Streams Failure Processor: Testing failure flow", new Exception());
    }

    @Override
    public String handleError(Exception ex, String payLoad, Map<String, String> headers) {
        LOGGER.error("Task failed successfully. Error handled.");
        return null;
    }
}
