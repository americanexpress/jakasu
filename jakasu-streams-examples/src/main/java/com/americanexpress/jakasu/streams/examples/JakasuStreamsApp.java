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

package com.americanexpress.jakasu.streams.examples;

import com.americanexpress.jakasu.subscriber.annotations.EnableJakasuStreams;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Main App to test Jakasu Kafka Streams Subscriber
 */
@SpringBootApplication
@EnableJakasuStreams
@EnableEncryptableProperties
public class JakasuStreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(JakasuStreamsApp.class);

    public static void main(String[] args) {
        logger.info("JakasuStreams Test App Starting.............");
        new SpringApplicationBuilder()
                .sources(JakasuStreamsApp.class).run(args);
    }
}
