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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * manages splitting of all config into individual topics
 * and handing each over to a SubscriberInitializer
 */
@Service
@Import({SubscriberInitializer.class, StreamProcessorInitializer.class})
public class ConfigRunnerManager {
    private static final Logger logger = LoggerFactory.getLogger(ConfigRunnerManager.class);
    @Autowired
    public ConfigLoader configLoader;
    @Autowired
    public SubscriberInitializer initializer;
    @Autowired
    public StreamProcessorInitializer streamProcessorInitializer;

    private ExecutorService service;

    /**
     * On start up, once config has loader into the ConfigLoader,
     * for each topic, initialize either subscriber or streams consumer
     * and submit to thread pool service
     * Each subscriber defined in config will run independently in its own thread
     */
    @PostConstruct
    public void createThreads() {
        Map<String, ConfigLoader.Sub> subscriberConfigs = configLoader.getSubs();
        service = Executors.newFixedThreadPool(subscriberConfigs.size());
        logger.info("Service thread pool created with size {}", subscriberConfigs.size());
        subscriberConfigs.entrySet().forEach(subsConfig -> {
            try {
                if (subsConfig.getValue().getStreamsEnable()) {
                    service.submit(() -> streamProcessorInitializer.initializer(subsConfig.getValue()).start());
                } else {
                    service.submit(() -> initializer.initContainer(subsConfig.getValue()).start());
                }
                logger.debug("Submit to service for {}", subsConfig.getValue().getTopicname());
            } catch (Exception e) {
                logger.error("Failed to submit/start config:{}", subsConfig);
            }
        });
    }

    /**
     * Shut down threads when app shuts down
     */
    @PreDestroy
    public void shutdown() {
        service.shutdown();
    }
}
