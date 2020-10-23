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
import com.americanexpress.jakasu.subscriber.helpers.RecoveryCallbackImpl;
import com.americanexpress.jakasu.subscriber.helpers.Util;
import com.americanexpress.jakasu.subscriber.services.SubscriberService;
import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class SubscriberInitializer {

    private static final Logger logger = LoggerFactory.getLogger(SubscriberInitializer.class);

    @Autowired
    Environment env;
    @Autowired
    ConfigurableApplicationContext context;

    /**
     * Set behaviour for retries from user config (default vals if not specified)
     * If request to a topic partition fails, how many times to retry and how
     * long to wait between tries
     *
     * @param subscriberConfig config for 1 Jakusu subscriber
     * @return template used to define Kafka Listeners
     */
    private static RetryTemplate getRetryTemplate(ConfigLoader.Sub subscriberConfig) {
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate
                .setRetryPolicy(new SimpleRetryPolicy(subscriberConfig.getTopicRetry()));
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(subscriberConfig.getTopicBackoffPeriod());
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }

    /**
     * Initialize a listener container for a topic and define what
     * happens when a message is received by its consumers
     * Listener container manages the consumers in the consumer group
     * and consumes concurrently a thread partition for each topic
     * <p>
     * Input topic -> Listener container
     * -> Listener (Jakusu SubscriberService) -> User defined Jakusu Subscriber
     *
     * @param subscriberConfig config for 1 subscriber
     * @return listener container for listening for messages on topic
     */
    public ConcurrentMessageListenerContainer initContainer(ConfigLoader.Sub subscriberConfig) {
        logger.debug("Beginning subscriber setup for topic {} class {}", subscriberConfig.getTopicname(), subscriberConfig.getClassname());
        ConcurrentMessageListenerContainer container = null;

        try {
            // get instance of user defined Subscriber
            Subscriber subscriber = register(subscriberConfig.getClassname());

            // service to hand off messages to Subscriber and manage errors
            SubscriberService subscriberService = new SubscriberService(subscriber);
            logger.debug("Created subscriber service {} for topic {}", subscriberService, subscriberConfig.getTopicname());

            // Factory for container creation
            ConcurrentKafkaListenerContainerFactory<String, String> factory
                    = new ConcurrentKafkaListenerContainerFactory<>();
            logger.debug("Created container factory {} for topic {}", factory, subscriberConfig.getTopicname());

            // set config for factory
            factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(getProps(subscriberConfig)));
            factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
            factory.setRetryTemplate(getRetryTemplate(subscriberConfig));

            // add filtering if desired
            if (subscriberConfig.isFiltered()) {
                setFilter(subscriberConfig, factory);
            }

            container = factory.createContainer(subscriberConfig.getTopicname());
            // give container to recovery handler so it can be stopped if error is fatal
            factory.setRecoveryCallback(new RecoveryCallbackImpl(subscriber, container));
            // set SubscriberService as the Listener
            container.setupMessageListener((AcknowledgingMessageListener<String, String>) subscriberService::serve);
            logger.debug("Configured container {} for topic {} with subscriber {}", container, subscriberConfig.getTopicname(), subscriber);
        } catch (Exception e) {
            logger.error("Fatal Error Shutting Down App ", e);
            System.exit(1);
        }
        return container;
    }

    /**
     * Apply a filter strategy to factory
     *
     * @param subscriberConfig user config
     * @param factory          Kafka listener factory on which to apply filter strategy
     */
    private void setFilter(ConfigLoader.Sub subscriberConfig,
                           ConcurrentKafkaListenerContainerFactory<String, String> factory) {
        logger.debug("Setting filter strategy");
        if (subscriberConfig.isCustomFiltered()) {
            try {
                Class filterClass = Class.forName(subscriberConfig.getFilterCustomClassname());
                final Filter filter = (Filter) filterClass.newInstance();
                factory.setRecordInterceptor(record -> filter.filter(record));
            } catch (Exception e) {
                //non fatal error, continue without filtering
                logger.error("ERROR: could not instantiate user defined filter class. Please specify fully qualified filter class name in configuration. Filtering will be skipped.");
            }
        } else {
            final Filter filter = new DefaultFilter(
                    subscriberConfig.getFilterKeys(), subscriberConfig.getFilterValues());
            factory.setRecordInterceptor(record -> filter.filter(record));
        }

    }

    /**
     * Create an instance of the user defined subscriber class
     * Created as beans not POJO so user can incorporate other
     * Spring components into their subscribers as needed
     *
     * @param subscriberName class name
     * @return new subscriber instance from bean factory
     */
    public Subscriber register(String subscriberName) {
        Subscriber subscriber;
        Class subClass = null;
        try {
            subClass = Class.forName(subscriberName);
            subscriber = (Subscriber) context.getAutowireCapableBeanFactory().createBean(subClass);
            logger.debug("Created subscriber {}", subscriber);
        } catch (IllegalStateException e) {
            logger.error("WARNING: Illegal state. Refreshing context");
            context.refresh();
            subscriber = (Subscriber) context.getAutowireCapableBeanFactory().createBean(subClass);
        } catch (Exception ex) {
            logger.error("Unable to instantiate subscriber for class {}. Please ensure this class exists in your project.", subscriberName);
            throw new InitializerException("Subscribe Instance creation Failed", ex);
        }
        return subscriber;
    }

    /**
     * Put properties from the config into a map to be used by the consumer factory
     *
     * @param subscriberConfig config for 1 subscriber
     * @return map of properties for consumer factory
     */
    private Map<String, Object> getProps(ConfigLoader.Sub subscriberConfig) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, subscriberConfig.getBootstrapServers());
        //how long after hearing nothing from this consumer to kick it out of consumer group
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, subscriberConfig.getTopicSessionTimeout());
        //how long to wait on a request
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, subscriberConfig.getTopicRequestTimeout());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, subscriberConfig.getTopicMaxPollRecords());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscriberConfig.getTopicResetConfig());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, subscriberConfig.getGroupid());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, subscriberConfig.getTopicBackoffPeriod());

        Util.INSTANCE.setSecurityProperties(props, env);

        return props;
    }
}
