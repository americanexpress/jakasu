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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * To implement a custom filtering strategy, implement this interface
 * and specify the class name in the config yaml under
 * *.subs.yoursubname.filter.custom-classname
 * <p>
 * Filter should take in a ConsumerRecord and return the same if
 * record should be processed, or null if it should be filtered out
 * Since you receive the raw Kafka record, you have the option to
 * filter on any part of the message body or its metadata
 * <p>
 * If you only wish to filter on headers, use the default
 * Jakasu filter by listing header keys and values under
 * *.subs.yoursubname.filter.keys and
 * *.subs.yoursubname.filter.values
 */
public interface Filter {
    ConsumerRecord filter(ConsumerRecord record);
}
