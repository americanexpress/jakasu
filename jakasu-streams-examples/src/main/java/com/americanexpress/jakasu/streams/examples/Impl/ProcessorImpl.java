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

import com.americanexpress.jakasu.subscriber.streams.StreamProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Example implementation of a Jakasu StreamProcessor class
 * Will read each message from the input topic,
 * prepend 'Modified by Jakasu streams:' to the payload,
 * and send to output topic.
 */

@Component
public class ProcessorImpl extends StreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorImpl.class);

    @Override
    public String process(String payload, Map<String, String> headers) {
        LOGGER.info("Streams processor implementation received {}", payload);
        return "Modified by Jakasu streams: " + payload;
    }

    @Override
    public String handleError(Exception ex, String payLoad, Map<String, String> headers) {
        return null;
    }

}
