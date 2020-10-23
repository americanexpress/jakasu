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

import com.americanexpress.jakasu.subscriber.exceptions.SubscriberException;
import com.americanexpress.jakasu.subscriber.helpers.Util;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Jakasu SDK user should implement these abstract methods in one or more
 * classes of StreamProcessor to use Jakasu Streams, which will execute
 * sequentially in the order in which they are listed in config
 */

public abstract class StreamProcessor implements Transformer {

    private static final Logger parentLogger = LoggerFactory.getLogger(StreamProcessor.class);

    private ProcessorContext context;

    /**
     * Get context for headers
     *
     * @param processorContext context
     */
    @Override
    public void init(ProcessorContext processorContext) {
        context = processorContext;
    }

    /**
     * Message is processed according to user's implementation of
     * process method and any errors are passed to error handler
     *
     * @param key   message key
     * @param value message value
     * @return transformed msg or null if could not be processed
     */
    @Override
    public Object transform(Object key, Object value) {
        String resultValue = null;
        Map<String, String> headers = Util.INSTANCE.getheadersAsMap(context.headers());
        try {
            resultValue = process(new String((byte[]) value), headers);
            transformHeaders(context.headers());
        } catch (Exception ex) {
            parentLogger.debug("exception caught for message : {}, with header: {} and exception is: {}", value, headers, ex);
            // intentionally given as debug. Actual error handling should be implemented in handleError method
            resultValue = handleError(ex, new String((byte[]) value), headers);
        }
        if (resultValue == null)
            return null;
        return new KeyValue(key, resultValue.getBytes());
    }

    @Override
    public void close() {
        //nothing to shut down
    }

    /**
     * IMPORTANT - Jakasu SDK users must
     * Implement logic for how messages should be processed
     *
     * @param payload msg body to transform
     * @param headers headers
     * @return transformed payload to be published in output topic (or sent to nest processor)
     */
    protected abstract String process(String payload, Map<String, String> headers) throws SubscriberException;

    /**
     * IMPORTANT - Jakasu SDK users must
     * Implement how to react to failure during processing
     *
     * @param ex      cause of failure
     * @param payload message body
     * @param headers message headers
     * @return message which has been processed in an alternate manner
     * and can be published/passed to next processor or null if message should be skipped
     */
    protected abstract String handleError(Exception ex, String payload, Map<String, String> headers);

    /**
     * Optionally implemented by user if headers should be transformed
     * Headers are contained in processor context, so they are modified but not returned
     *
     * @param headers message headers
     */
    protected void transformHeaders(Headers headers) {
        //do nothing - by default
    }
}
