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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.core.env.Environment;

import java.util.HashMap;
import java.util.Map;


public enum Util {
    /**
     * Common utilities for subscribers and streams
     */

    INSTANCE;

    /**
     * convert a Kafka Headers object to a Map
     *
     * @param recordHeaders Headers obj from Kafka
     * @return headers in a map for easier processing
     */
    public Map<String, String> getheadersAsMap(Headers recordHeaders) {
        Map<String, String> headers = new HashMap<>();
        recordHeaders.forEach(header -> headers.put(header.key(), new String(header.value())));
        return headers;
    }

    /**
     * Security properties for consumer factory which should be global
     *
     * @param props       props map to add to
     * @param environment for security settings in yaml
     */
    public void setSecurityProperties(Map<String, Object> props, Environment environment) {

        if (Boolean.valueOf(environment.getProperty("jakasu.security.enabled"))) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, environment.getProperty("jakasu.security.protocol"));
            props.put(SslConfigs.SSL_PROTOCOL_CONFIG, environment.getProperty("jakasu.security.ssl.protocol"));
            props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, environment.getProperty("jakasu.security.ssl.keystore.type"));
            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, environment.getProperty("jakasu.security.ssl.keystore.location"));
            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, environment.getProperty("jakasu.security.ssl.keystore.password"));
            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, environment.getProperty("jakasu.security.ssl.key.password"));
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, environment.getProperty("jakasu.security.ssl.truststore.location"));
            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, environment.getProperty("jakasu.security.ssl.truststore.password"));
        }
    }

    /**
     * String headers for logging instead of an unreadable byte array
     *
     * @param headers headers
     * @return headers as a string
     */
    public String headersBytesToString(Headers headers) {
        StringBuilder headersStr = new StringBuilder();
        for (Header header : headers) {
            headersStr.append(header.key());
            headersStr.append(": ");
            headersStr.append(new String(header.value()));
            headersStr.append(", ");
        }
        return headersStr.toString();
    }
}
