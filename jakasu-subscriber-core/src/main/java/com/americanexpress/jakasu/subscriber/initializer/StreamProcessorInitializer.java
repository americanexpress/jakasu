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

import com.americanexpress.jakasu.subscriber.exceptions.InitializerException;
import com.americanexpress.jakasu.subscriber.filter.DefaultFilter;
import com.americanexpress.jakasu.subscriber.filter.Filter;
import com.americanexpress.jakasu.subscriber.helpers.Util;
import com.americanexpress.jakasu.subscriber.streams.DeduplicationTransformer;
import com.americanexpress.jakasu.subscriber.streams.FilterTransformer;
import com.americanexpress.jakasu.subscriber.streams.FlatmapTransformer;
import com.americanexpress.jakasu.subscriber.streams.StreamProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Properties;

@Component
public class StreamProcessorInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamProcessorInitializer.class);

    @Autowired
    ConfigurableApplicationContext applicationContext;

    @Autowired
    Environment env;

    /**
     * Initializes a Jakusu streams subscriber
     * Defines streams topology of a Kafka StreamsBuilder,
     * input topic as source -> user defined processors -> output topic as sink
     *
     * @param subscriberConfig config for one
     * @return a KafkaStreams object built from the topology with the user defined config
     */
    public KafkaStreams initializer(ConfigLoader.Sub subscriberConfig) {
        // empty Kafka builder
        StreamsBuilder builder = new StreamsBuilder();

        try {
            //set input topic
            KStream stream = builder.stream(subscriberConfig.getTopicname());

            if (subscriberConfig.isFiltered()) {
                stream = setFilter(subscriberConfig, stream);
            }

            if (subscriberConfig.isDedup()) {
                stream = setDedup(subscriberConfig, stream, builder);
            }

            stream = setUserProcessors(subscriberConfig, stream);

            if (subscriberConfig.isFlatMapEnabled()) {
                stream = setFlatmap(subscriberConfig, stream);
            }

            // set output topic
            stream.to(subscriberConfig.getOutputTopicName());

        } catch (Exception e) {
            LOGGER.error("Fatal Error Shutting Down App ", e);
            System.exit(1);
        }
        return null;
    }

    public KafkaStreams createRunnableStreams(Topology topology, Properties properties) {
        return new KafkaStreams(topology, properties);
    }

    /**
     * create each user defined processor and add it to topology
     *
     * @param subscriberConfig user config
     * @param stream           KStream on which to add user processors
     * @return
     */
    public KStream setUserProcessors(ConfigLoader.Sub subscriberConfig, KStream stream) {
        String[] streamProcessors = subscriberConfig.getStreamsProcessor().split(",");
        for (String streamProcessor : streamProcessors) {
            stream = stream.transform(() -> createProcessor(streamProcessor));
        }
        return stream;
    }

    /**
     * Apply a filter strategy to topology
     *
     * @param subscriberConfig user config
     * @param stream           KStream on which to add FilterTransformer
     */
    private KStream setFilter(ConfigLoader.Sub subscriberConfig, KStream stream) {
        LOGGER.debug("Setting filter strategy");
        if (subscriberConfig.isCustomFiltered()) {
            try {
                Class filterClass = Class.forName(subscriberConfig.getFilterCustomClassname());
                final Filter filter = (Filter) applicationContext.getAutowireCapableBeanFactory().createBean(filterClass);
                return stream.transform(() -> new FilterTransformer(filter));
            } catch (Exception e) {
                throw new InitializerException("ERROR: could not instantiate user defined filter class. Please specify fully qualified filter class name in configuration.", e);
            }
        } else {
            final Filter filter = new DefaultFilter(
                    subscriberConfig.getFilterKeys(), subscriberConfig.getFilterValues());
            return stream.transform(() -> new FilterTransformer(filter));
        }
    }

    /**
     * Apply a deduplication strategy to topology
     *
     * @param subscriberConfig user config
     * @param stream           KStream on which to add DeduplicationTransformer
     */
    private KStream setDedup(ConfigLoader.Sub subscriberConfig,
                             KStream stream, StreamsBuilder builder) {
        //if id header doesn't exist, skip dedup setup
        if (subscriberConfig.getDedupHeader() == null) {
            throw new InitializerException("ERROR initializing streams deduplication strategy: missing id-header. Specify which header to use as unique id.");
        } else {
            LOGGER.debug("Setting deduplication transformer");
            // set internal topic and add dedup transformer to topology
            Duration windowSize = Duration.ofDays(subscriberConfig.getDedupWindow());
            setDedupStore(builder, windowSize);
            return stream.transform(() -> new DeduplicationTransformer<>(
                    windowSize.toMillis(), subscriberConfig.getDedupHeader()), "eventId-store");
        }
    }

    /**
     * Apply a flatmap strategy to topology
     *
     * @param subscriberConfig user config
     * @param stream           KStream on which to add FlatmapTransformer
     */
    private KStream setFlatmap(ConfigLoader.Sub subscriberConfig, KStream stream) {
        //if id header doesn't exist, skip dedup setup
        if (subscriberConfig.getFlatmapHeader() == null) {
            //not a fatal error - continue setup without dedup
            throw new InitializerException("ERROR initializing streams flat mapping: missing id-header. Specify which header to use as unique id.");
        } else {
            LOGGER.debug("Setting flatmap transformer");
            // set internal topic and add flatmap transformer to topology
            return stream.transform(() -> new FlatmapTransformer(
                    subscriberConfig.getFlatmapHeader()));
        }
    }

    /**
     * Create an instance of the user defined Processor class
     * Created as beans not POJO so user can incorporate other
     * Spring components into their processors as needed
     *
     * @param streamProcessorName class name
     * @return new instance created from bean factory
     */
    public StreamProcessor createProcessor(String streamProcessorName) {
        StreamProcessor processor;
        Class<StreamProcessor> processorClass = null;
        try {
            processorClass = (Class<StreamProcessor>) Class.forName(streamProcessorName);
            processor = applicationContext.getAutowireCapableBeanFactory().createBean(processorClass);
        } catch (IllegalStateException e) {
            LOGGER.error("WARNING: Illegal state. Refreshing context");
            applicationContext.refresh();
            processor = applicationContext.getAutowireCapableBeanFactory().createBean(processorClass);
        } catch (Exception ex) {
            throw new InitializerException("Stream Processor Instance creation Failed", ex);
        }
        return processor;
    }

    /**
     * Set an internal topic for this StreamsBuilder to track seen messages
     * for deduplication comparison
     * Topic will be created if it doesn't exist already, so application
     * must be authorized to create topics in the cluster or
     * topic must be created manually beforehand
     *
     * @param kStreamBuilder builder to use
     * @param windowSize     # of days to check for duplicates
     */
    private void setDedupStore(StreamsBuilder kStreamBuilder, Duration windowSize) {
        // retention period must be at least window size, so use same # for both
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore("eventId-store", windowSize, windowSize, false),
                Serdes.String(),
                Serdes.Long());

        kStreamBuilder.addStateStore(dedupStoreBuilder);
    }

    /**
     * Load from config object into properties object which is expected by KafkaStreams
     *
     * @param subscriberConfig config for 1 streams subscriber
     * @return Properties map as expected by KafkaStreams
     */
    public Properties loadConsumerConfig(ConfigLoader.Sub subscriberConfig) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, subscriberConfig.getBootstrapServers());
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, subscriberConfig.getGroupid());

        //optional properties
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, subscriberConfig.getStreamsProcessGuarentee());
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, subscriberConfig.getStreamsTopologyOptimize());
        props.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, subscriberConfig.getTopicBackoffPeriod());
        props.put(StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, subscriberConfig.getStreamsBufferRecordsPerPartition());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, subscriberConfig.getStreamsMaxBytesBuffered());
        props.put(StreamsConfig.POLL_MS_CONFIG, subscriberConfig.getPollMS());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, subscriberConfig.getStreamsReplication());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, subscriberConfig.getStreamsThreadCount());
        props.put(StreamsConfig.RETRIES_CONFIG, subscriberConfig.getTopicRetry());
        //how long after hearing nothing from this consumer to kick it out of consumer group
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, subscriberConfig.getTopicSessionTimeout());
        //how long to wait on a request
        props.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, subscriberConfig.getTopicRequestTimeout());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, subscriberConfig.getTopicMaxPollRecords());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscriberConfig.getTopicResetConfig());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        //security props set in hashmap
        HashMap<String, Object> securityProps = new HashMap<>();
        Util.INSTANCE.setSecurityProperties(securityProps, env);
        props.putAll(securityProps);

        LOGGER.debug("loaded all kafka streams properties : {}", props);

        return props;
    }

}
