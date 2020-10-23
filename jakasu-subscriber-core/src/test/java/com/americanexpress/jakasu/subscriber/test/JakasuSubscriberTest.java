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

package com.americanexpress.jakasu.subscriber.test;

import com.americanexpress.jakasu.subscriber.exceptions.InitializerException;
import com.americanexpress.jakasu.subscriber.filter.DefaultFilter;
import com.americanexpress.jakasu.subscriber.helpers.Util;
import com.americanexpress.jakasu.subscriber.initializer.ConfigLoader;
import com.americanexpress.jakasu.subscriber.initializer.ConfigRunnerManager;
import com.americanexpress.jakasu.subscriber.initializer.StreamProcessorInitializer;
import com.americanexpress.jakasu.subscriber.initializer.SubscriberInitializer;
import com.americanexpress.jakasu.subscriber.services.SubscriberService;
import com.americanexpress.jakasu.subscriber.setup.TestFailureSubscriber;
import com.americanexpress.jakasu.subscriber.setup.TestProcessor;
import com.americanexpress.jakasu.subscriber.setup.TestSubscriber;
import com.americanexpress.jakasu.subscriber.streams.DeduplicationTransformer;
import com.americanexpress.jakasu.subscriber.streams.FilterTransformer;
import com.americanexpress.jakasu.subscriber.streams.FlatmapTransformer;
import com.americanexpress.jakasu.subscriber.subscribe.Subscriber;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.test.context.ContextConfiguration;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@EmbeddedKafka
@SpringBootTest
@ContextConfiguration(classes = {ConfigLoader.class,
        SubscriberInitializer.class, StreamProcessorInitializer.class})
public class JakasuSubscriberTest {

    @Autowired
    ConfigLoader configLoader;

    @Autowired
    SubscriberInitializer subscriberInitializer;

    @Autowired
    StreamProcessorInitializer streamProcessorInitializer;

    @Test
    public void configLoaderTest() {
        assertNotNull(configLoader);
        Map<String, ConfigLoader.Sub> subsConfig = configLoader.getSubs();
        assertNotNull(subsConfig);
    }

    @Test
    public void runnerManagerTest() {
        ConfigRunnerManager runnerManager = new ConfigRunnerManager();
        runnerManager.configLoader = configLoader;
        StreamProcessorInitializer streamProcessorInitializer = mock(StreamProcessorInitializer.class);
        SubscriberInitializer subscriberInitializer = mock(SubscriberInitializer.class);
        assertTrue(Mockito.mockingDetails(streamProcessorInitializer).isMock());
        assertTrue(Mockito.mockingDetails(subscriberInitializer).isMock());
        try {
            runnerManager.createThreads(); //will just submit null to executor service
            runnerManager.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e);
        }
    }

    @Test
    public void subscriberInitializerTest() {
        ConfigLoader.Sub config = configLoader.getSubs().get("test");
        assertNotNull(config);
        ConcurrentMessageListenerContainer container = subscriberInitializer.initContainer(config);
        assertNotNull(container);

        config = configLoader.getSubs().get("test2");
        assertNotNull(config);
        container = subscriberInitializer.initContainer(config);
        assertNotNull(container);

        //test fail because subscriber class doesn't exist
        assertThrows(InitializerException.class,
                () -> subscriberInitializer.register("nonexistent-subscriber-class"),
                "Subscribe Instance creation Failed");
    }

    @Test
    public void streamsInitTest() {
        ConfigLoader.Sub config = configLoader.getSubs().get("testStreams");
        assertNotNull(config);
        StreamProcessorInitializer streamInitSpy = spy(streamProcessorInitializer);
        assertNotNull(streamProcessorInitializer);
        // will try creating KafkaAgent so ignore this method
        doReturn(null).when(streamInitSpy).createRunnableStreams(any(), any());

        try {
            assertNull(streamInitSpy.initializer(config));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e);
        }

        config = configLoader.getSubs().get("testStreams2");
        try {
            assertNull(streamInitSpy.initializer(config));
        } catch (Exception e) {
            e.printStackTrace();
            fail(e);
        }

        //test fail because processor class doesn't exist
        assertThrows(InitializerException.class,
                () -> streamProcessorInitializer.createProcessor("nonexistent-processor-class"),
                "Stream Processor Instance creation Failed");

    }

    @Test
    public void subscriberServiceTest() {
        Subscriber subscriber = mock(TestSubscriber.class);
        SubscriberService service = new SubscriberService(subscriber);

        ConsumerRecord<String, String> record =
                new ConsumerRecord<String, String>("topic", 0, 0L,
                        "Key", "val");

        Acknowledgment ack = new Acknowledgment() {
            @Override
            public void acknowledge() {
                return;
            }

            @Override
            public void nack(long sleep) {
                return;
            }
        };
        service.serve(record, ack);
        try {
            verify(subscriber, times(1)).subscribe(anyString(), anyMap());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e);
        }

        Subscriber failSubscriber = spy(TestFailureSubscriber.class);
        service = new SubscriberService(failSubscriber);
        service.serve(record, ack);
        try {
            verify(failSubscriber, times(1)).handleError(any());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e);
        }

    }

    @Test
    public void dedupTransformerTest() {
        StringSerializer serializer = new StringSerializer();
        StringDeserializer deserializer = new StringDeserializer();
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(serializer, serializer);

        StreamsBuilder builder = new StreamsBuilder();
        Duration duration = Duration.ofMinutes(7);
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore("eventId-store", duration, duration, false),
                Serdes.String(),
                Serdes.Long()));
        builder.stream("test-topic").transform(() -> new DeduplicationTransformer<>(10, "unique-id"), "eventId-store")
                .to("output-topic");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);

        //unique
        Headers headers = new RecordHeaders().add("unique-id", "testEvent1".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "test", headers);

        //duplicate
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        assertNull(testDriver.readOutput("output-topic", deserializer, deserializer));

        //unique
        headers = new RecordHeaders().add("unique-id", "testEvent2".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test2", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "test2", headers);

        //unique id not present
        headers = new RecordHeaders().add("testHeader2", "testHeaderVal".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "test", headers);

        testDriver.close();
    }

    @Test
    public void testFilter() {
        ConfigLoader.Sub config = configLoader.getSubs().get("testStreams");
        String keys = config.getFilterKeys();
        String vals = config.getFilterValues();
        Map<String, String> headers = new HashMap<>();

        DefaultFilter filter = new DefaultFilter(keys, vals);

        headers.put("test", "test");
        assertFalse(filter.isMatchedForFilter(headers));
        headers.put("id", "id1");
        headers.put("location", "location1");
        assertTrue(filter.isMatchedForFilter(headers));
        headers.put("ignore", "ignore");
        assertTrue(filter.isMatchedForFilter(headers));
        headers.put("id", "id2");
        headers.put("location", "location2");
        assertTrue(filter.isMatchedForFilter(headers));
        headers.remove("id");
        assertFalse(filter.isMatchedForFilter(headers));
    }

    @Test
    public void filterTransformerTest() {
        StringSerializer serializer = new StringSerializer();
        StringDeserializer deserializer = new StringDeserializer();
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(serializer, serializer);
        ConfigLoader.Sub config = configLoader.getSubs().get("testStreams");
        String keys = config.getFilterKeys();
        String vals = config.getFilterValues();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-topic").transform(() -> new FilterTransformer<>(new DefaultFilter(keys, vals)))
                .to("output-topic");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);

        //match filter
        Headers headers = new RecordHeaders().add("id", "id1".getBytes()).add("location", "location1".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "test", headers);

        //not filter
        headers = new RecordHeaders().add("id", "id1".getBytes()).add("location", "location2".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        assertNull(testDriver.readOutput("output-topic", deserializer, deserializer));

        //not filter
        headers = new RecordHeaders().add("type", "type1".getBytes()).add("timestamp", "time1".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        assertNull(testDriver.readOutput("output-topic", deserializer, deserializer));

        //filter
        headers = new RecordHeaders().add("id", "id2".getBytes()).add("location", "location2".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "test", headers);

        //not filter
        headers = new RecordHeaders().add("testHeader2", "testHeaderVal".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        assertNull(testDriver.readOutput("output-topic", deserializer, deserializer));

        testDriver.close();
    }

    @Test
    public void flatmapTransformerTest() {
        StringSerializer serializer = new StringSerializer();
        StringDeserializer deserializer = new StringDeserializer();
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(serializer, serializer);
        ConfigLoader.Sub config = configLoader.getSubs().get("testStreams");

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-topic").transform(() -> new FlatmapTransformer(config.getFlatmapHeader()))
                .to("output-topic");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);

        //not array
        Headers headers = new RecordHeaders().add("id", "id1".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "This is not the proper format.", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "This is not the proper format.", headers);

        //split array
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "[{\"test\":\"test1\"},{\"test\":\"test2\"}]", headers));
        headers = new RecordHeaders().add("id", "id1-0".getBytes());
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "{\"test\":\"test1\"}", headers);
        headers = new RecordHeaders().add("id", "id1-1".getBytes());
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer), "test", "{\"test\":\"test2\"}", headers);

        testDriver.close();
    }

    @Test
    public void streamsProcessorTest() {
        StringSerializer serializer = new StringSerializer();
        StringDeserializer deserializer = new StringDeserializer();
        ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(serializer, serializer);

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-topic").transform(() -> new TestProcessor())
                .to("output-topic");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-group");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), props);

        Headers headers = new RecordHeaders().add("header1", "header1".getBytes());
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "test", headers));
        headers = new RecordHeaders().add("header1", "header1".getBytes()).add("test", "testVal".getBytes());
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer),
                "test", "TEST", headers);
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "1", headers));
        assertNull(testDriver.readOutput("output-topic", deserializer, deserializer));
        testDriver.pipeInput(recordFactory.create("test-topic", "test", "fail", headers));
        OutputVerifier.compareKeyValueHeaders(testDriver.readOutput("output-topic", deserializer, deserializer),
                "test", "ERROR: fail", headers);

        testDriver.close();
    }

    @Test
    public void securityPropsTest() {
        Map<String, Object> props = new HashMap();
        Environment env = new MockEnvironment()
                .withProperty("jakasu.security.protocol", "test")
                .withProperty("jakasu.security.enabled", "true")
                .withProperty("jakasu.security.ssl.protocol", "test")
                .withProperty("jakasu.security.ssl.keystore.type", "test")
                .withProperty("jakasu.security.ssl.keystore.location", "test")
                .withProperty("jakasu.security.ssl.keystore.password", "test")
                .withProperty("jakasu.security.ssl.key.password", "test")
                .withProperty("jakasu.security.ssl.truststore.location", "test")
                .withProperty("jakasu.security.ssl.truststore.password", "test");
        Util.INSTANCE.setSecurityProperties(props, env);
        assertTrue(props.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_PROTOCOL_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_KEY_PASSWORD_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
        assertTrue(props.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    }

    @Test
    public void headerBytesToStringTest() {
        String headersString = Util.INSTANCE.headersBytesToString(
                new RecordHeaders().add("header1", "header1".getBytes())
                        .add("header2", "header2".getBytes()));
        assertEquals("header1: header1, header2: header2, ", headersString);
    }
}

