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

package com.americanexpress.jakasu.subscriber.examples;

import com.americanexpress.jakasu.subscriber.filter.Filter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Example of implementing a custom filter strategy
 * This filter ignores messages which contain "filter" in the payload
 * <p>
 * You can filter on message key or payload or combination.
 * To filter only on headers, use the default, config driven
 * Jakasu implementation of Filter
 */
public class ExampleCustomFilter implements Filter {
    @Override
    public ConsumerRecord filter(ConsumerRecord record) {
        if (record.value().toString().contains("filter")) {
            return null; //return null if message should not be processed
        }
        return record;
    }
}
