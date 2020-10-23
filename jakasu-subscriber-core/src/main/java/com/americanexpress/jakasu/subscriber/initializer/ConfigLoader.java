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

package com.americanexpress.jakasu.subscriber.initializer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 * Gets configuration from yaml file.
 * Must match the format
 * <p>
 * <p>
 * jakasu:
 * subs:
 * <p>
 * # SUBSCRIBER:
 * subscriber-example:
 * classname: com.americanexpress.jakasu.subscriber.examples.subscriber.TestSubscriber2
 * groupid: "example_consumer_group_id"
 * bootstrap-servers: yourhost:port
 * topic:
 * name: "Test-Topic2"
 * <p>
 * # OPTIONAL CONFIG:
 * session-timeout: 30000
 * request-timeout: 30000
 * max-poll-records: 1
 * retry: 3
 * backoff-period: 20
 * reset-config: "latest"
 * filter:
 * keys: source-type,event-type
 * values: (SourceA,EventType1),(SourceB,EventType2)
 * <p>
 * # STREAMS
 * streams-example:
 * topic:
 * name: "Input-Topic"
 * groupid: "example_consumer_group_id"
 * bootstrap-servers: yourserver:8080
 * streams:
 * enable: true
 * processor: com.americanexpress.jakasu.streams.examples.ProcessorImpl
 * output-topic-name: "Test-Output-Topic"
 * <p>
 * # OPTIONAL config:
 * thread-count: 3
 * poll-ms: 150
 * process-guarantee: exactly_once
 * topology-optimize: all
 * buffer-records-per-partition: 500
 * max-bytes-buffered: 1000000
 * dedup:
 * enable: true
 * id-header: "source-uniqueid"
 * window-size: 7
 * replication: 4
 * filter:
 * custom-classname: com.americanexpress.jakasu.streams.examples.CustomFilter
 */

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "jakasu")
public class ConfigLoader {

    private Map<String, Sub> subs;

    public Map<String, Sub> getSubs() {
        return subs;
    }

    public void setSubs(Map<String, Sub> subs) {
        this.subs = subs;
    }

    @Configuration
    public static class Sub {
        private String bootstrapServers;
        //  location for connecting to the kafka cluster.
        private String groupid;
        // a consumer groupid which is unique to your application.
        private String classname;
        private Map<String, String> filter;
        // config for filtering
        private Map<String, String> streams;
        // config for streams
        private Map<String, String> topic;
        // name of the kafka topic to consume from

        public String getGroupid() {
            return groupid;
        }

        public void setGroupid(String groupid) {
            this.groupid = groupid;
        }

        public String getClassname() {
            return classname;
        }

        public void setClassname(String classname) {
            this.classname = classname;
        }

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        /**
         * Keys and values for which messages to extract from the input stream.
         * Keys should be listed as comma separated strings and values should be listed as comma separated tuples.
         *
         * @return map of filter config
         */
        public Map<String, String> getFilter() {
            return filter;
        }

        public void setFilter(Map<String, String> filter) {
            this.filter = filter;
        }

        public boolean isFiltered() {
            return this.filter != null;
        }

        public Map<String, String> getStreams() {
            return streams;
        }

        public void setStreams(Map<String, String> streams) {
            this.streams = streams;
        }

        public Map<String, String> getTopic() {
            return topic;
        }

        public void setTopic(Map<String, String> topic) {
            this.topic = topic;
        }

        /**
         * provide as many keys for filtering as needed, with each key separated by a comma.
         *
         * @return the keys
         */
        public String getFilterKeys() {
            if (this.filter == null || !this.filter.containsKey("keys")) {
                return "";
            }
            return this.getFilter().get("keys");
        }

        /**
         * provide the value to each of the specified keys above as comma separated tuples
         *
         * @return the values
         */
        public String getFilterValues() {
            if (this.filter == null || !this.filter.containsKey("values")) {
                return "";
            }
            return this.getFilter().get("values");
        }

        /**
         * Whether the user has implemented their own filter strategy or is using the default
         *
         * @return is using a custom filter?
         */
        public boolean isCustomFiltered() {
            return this.filter != null && this.filter.containsKey("custom-classname");
        }

        /**
         * get the name of the class of the user implemented filter strategy if they do not
         * want to use the default provided by FilterUtil
         *
         * @return filter class name
         */
        public String getFilterCustomClassname() {
            if (isCustomFiltered()) {
                return this.getFilter().get("custom-classname");
            }
            return null;
        }

        /**
         * must be set as “true” to use JakusuStreams
         *
         * @return is streams
         */
        public boolean getStreamsEnable() {
            if (this.streams != null && this.streams.containsKey("enable")) {
                return this.streams.get("enable").equalsIgnoreCase("true");
            }
            return false;
        }

        /**
         * if the broker doesn’t hear from the consumer in this amount of ms,
         * it considers the consumer dead and rebalances the cluster.
         * default: 10000 ms
         *
         * @return timeout
         */
        public Integer getTopicSessionTimeout() {
            if (this.topic.containsKey("session-timeout")) {
                return Integer.parseInt(this.topic.get("session-timeout"));
            }
            return 10000;
        }

        public String getTopicname() {
            return this.topic.get("name");
        }


        /**
         * amount of ms the consumer will wait for a response to a request before retrying.
         * default: 30000 ms
         *
         * @return timeout
         */
        public Integer getTopicRequestTimeout() {
            if (this.topic.containsKey("request-timeout")) {
                return Integer.parseInt(this.topic.get("request-timeout"));
            }
            return 30000;
        }

        /**
         * max number of records returned in each poll
         * default: 500 records
         *
         * @return # of records
         */
        public Integer getTopicMaxPollRecords() {
            if (this.topic.containsKey("max-poll-records")) {
                return Integer.parseInt(this.topic.get("max-poll-records"));
            }
            return 500;
        }

        /**
         * number of times to retry a request if there is no response
         * default: 2147483647 tries
         *
         * @return # of tries
         */
        public Integer getTopicRetry() {
            if (this.topic.containsKey("retry")) {
                return Integer.parseInt(this.topic.get("retry"));
            }
            return 2147483647;
        }

        /**
         * amount of ms to wait before attempting to retry a failed request
         * default: 100 ms
         *
         * @return backoff period
         */
        public Integer getTopicBackoffPeriod() {
            if (this.topic.containsKey("backoff-period")) {
                return Integer.parseInt(this.topic.get("backoff-period"));
            }
            return 100;
        }

        /**
         * Offset from which a new consumer should read.
         * “latest” means the consumer only reads records from the queue which arrive after its creation.
         * “earliest” means the consumer will read all records including those which already exist in the queue before its creation.
         * default: latest
         *
         * @return reset type
         */
        public String getTopicResetConfig() {
            if (this.topic.containsKey("reset-config")) {
                return this.topic.get("reset-config");
            }
            return "latest";
        }

        /**
         * Name of the class which extends StreamProcessor in your project.
         * You can chain multiple processors together by listing them comma separated
         * in the order in which they should execute.
         * If you are subscribing to multiple topics, use a different processor class for each
         * if messages should be processed differently for each topic.
         * Otherwise, you can choose the same processor class for your topics,
         * and a unique instance will be created for each.
         *
         * @return class name
         */
        public String getStreamsProcessor() {
            return this.streams.get("processor");
        }

        /**
         * The Kafka topic to which the processed message should be published.
         * You must be authorized as a publisher for this topic.
         *
         * @return topic name
         */
        public String getOutputTopicName() {
            return this.streams.get("output-topic-name");
        }

        /**
         * The replication factor of internal topics that Kafka Streams creates
         * if using deduplication. This is the number of broker failures
         * which can be tolerated
         * recommended: 3
         *
         * @return replication factor
         */
        public Integer getStreamsReplication() {
            if (this.streams.containsKey("dedup.replication")) {
                return Integer.parseInt(this.streams.get("dedup.replication"));
            }
            return 3;
        }

        /**
         * The number of threads per topic to execute stream processing
         *
         * @return thread count
         */
        public Integer getStreamsThreadCount() {
            if (this.streams.containsKey("thread-count")) {
                return Integer.parseInt(this.streams.get("thread-count"));
            }
            return 1;
        }

        /**
         * The amount of time in milliseconds to block waiting for input while polling for records.
         * default: 100 ms
         *
         * @return polling ms
         */
        public Integer getPollMS() {
            if (this.streams.containsKey("poll-ms")) {
                return Integer.parseInt(this.streams.get("poll-ms"));
            }
            return 100;
        }

        /**
         * “at_least_once” or “exactly_once”.
         * Set to “exactly_once” to guarantee that each record is processed once and only once,
         * even if some failures are encountered in the middle of processing.
         * default: “at_least_once”
         *
         * @return guarentee
         */
        public String getStreamsProcessGuarentee() {
            if (this.streams.containsKey("process-guarantee")) {
                return this.streams.get("process-guarantee");
            }
            return "at_least_once";
        }

        /**
         * “all” or “none”. “all” reduces the footprint Kafka Streams creates especially on repartition.
         * default: “none”
         *
         * @return optimization
         */
        public String getStreamsTopologyOptimize() {
            if (this.streams.containsKey("topology-optimize")) {
                return this.streams.get("topology-optimize");
            }
            return "none"; //or all
        }

        /**
         * Set to true if duplicate messages should be ignored
         * Creates an internal topic which is named after the consumer group
         * To check if a message has been seen
         *
         * @return is dedup
         */
        public boolean isDedup() {
            return (this.streams.containsKey("dedup.enable")
                    && Boolean.parseBoolean(this.streams.get("dedup.enable")));
        }

        /**
         * The header whose value should be used as a unique identifier
         * for message deduplication
         *
         * @return header key or null if absent
         */
        public String getDedupHeader() {
            if (this.streams.containsKey("dedup.id-header")) {
                return this.streams.get("dedup.id-header");
            }
            return null;
        }

        /**
         * Number of days to retain a message in internal
         * dedup topic to check if messages are unique
         * default: 7 days (this is the default for all kafka topics)
         *
         * @return # of days
         */
        public Integer getDedupWindow() {
            if (this.streams.containsKey("dedup.window-size")) {
                return Integer.parseInt(this.streams.get("dedup.window-size"));
            }
            return 7;
        }

        /**
         * The maximum number of records to buffer per partition.
         * default: 1000 records
         *
         * @return # of records
         */
        public Integer getStreamsBufferRecordsPerPartition() {
            if (this.streams.containsKey("buffer-records-per-partition")) {
                return Integer.parseInt(this.streams.get("buffer-records-per-partition"));
            }
            return 1000;
        }

        /**
         * Maximum number of memory bytes to be used for record caches across all threads.
         * default: 10485760 bytes
         *
         * @return # of bytes
         */
        public Integer getStreamsMaxBytesBuffered() {
            if (this.streams.containsKey("max-bytes-buffered")) {
                return Integer.parseInt(this.streams.get("max-bytes-buffered"));
            }
            return 10485760;
        }

        /**
         * Set to true if messages are in a bulk json format
         * containing multiple records which should be split
         * into an individual message per record to publish to
         * the output topic
         *
         * @return is flat map enabled
         */
        public boolean isFlatMapEnabled() {
            return (this.streams.containsKey("flatmap.enable")
                    && Boolean.parseBoolean(this.streams.get("flatmap.enable")));
        }

        /**
         * The header whose value should be used as a unique identifier
         * for message flat mapping
         *
         * @return header key or null if absent
         */
        public String getFlatmapHeader() {
            if (this.streams.containsKey("flatmap.id-header")) {
                return this.streams.get("flatmap.id-header");
            }
            return null;
        }
    }
}
