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

package com.americanexpress.jakasu.subscriber.filter;

import com.americanexpress.jakasu.subscriber.helpers.Util;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultFilter implements Filter {
    public static final String REGEX = "\\((.*?)\\)";
    final static Pattern pattern = Pattern.compile(REGEX, Pattern.MULTILINE);
    private String acceptedHeaderKeys;
    private String acceptedHeaderVals;

    /**
     * Default Jakasu filtering strategy. Uses header keys and values given by
     * user in the configuration file.
     * Messages which have headers that match will be retained and others will be ignored
     *
     * @param acceptedHeaderKeys comma separated list of header keys
     * @param acceptedHeaderVals comma separated list of tuples for header values
     */
    public DefaultFilter(String acceptedHeaderKeys, String acceptedHeaderVals) {
        this.acceptedHeaderKeys = acceptedHeaderKeys;
        this.acceptedHeaderVals = acceptedHeaderVals;
    }

    /**
     * Apply filter to record
     *
     * @param record to check
     * @return original record or null if filtered out
     */
    @Override
    public ConsumerRecord filter(ConsumerRecord record) {
        Map<String, String> headers = Util.INSTANCE.getheadersAsMap(record.headers());
        if (!isMatchedForFilter(headers)) {
            return null;
        }
        return record;
    }

    /**
     * Compare message headers to see if they match the pattern for
     * values specified in the config yaml for filtering.
     * True for matching headers
     * example:
     * <p>
     * filter:
     * keys: source-type,event-type
     * values: (SourceA,TypeOfEvent1),(SourceB,TypeOfEvent2)
     * <p>
     * Returns true for headers source-type: SourceA, event-type: TypeOfEvent1
     * OR source-type: SourceB, event-type: TypeOfEvent2
     *
     * @param messageHeaders headers
     * @return message matches
     */
    public boolean isMatchedForFilter(Map<String, String> messageHeaders) {
        final Matcher matcher = pattern.matcher(acceptedHeaderVals);

        String[] headerKeys = acceptedHeaderKeys.split(",");

        List<Boolean> interClauseBooleans = new ArrayList<Boolean>();
        while (matcher.find()) {
            List<Boolean> intraClauseBooleans = new ArrayList<Boolean>();

            String values = matcher.group(1);
            String[] headerValues = values.split(",");

            if (headerKeys.length != headerValues.length) {
                return false;
            }
            for (int i = 0; i < headerKeys.length; i++) {
                if (!messageHeaders.containsKey(headerKeys[i])) {
                    return false;
                }
                intraClauseBooleans.add(messageHeaders.get(headerKeys[i]).equalsIgnoreCase(headerValues[i]));
            }
            interClauseBooleans.add(intraClauseBooleans.stream().reduce((x1, x2) -> ((Boolean) x1 && (Boolean) x2)).orElse(false));
        }

        return interClauseBooleans.stream().reduce((x1, x2) -> (Boolean) x1 || (Boolean) x2).orElse(false);
    }
}
