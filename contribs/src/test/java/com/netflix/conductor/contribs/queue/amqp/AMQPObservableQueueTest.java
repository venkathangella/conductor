/*
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.amqp;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.mockito.Mockito;

import com.netflix.conductor.contribs.queue.amqp.AMQPObservableQueue.Builder;
import com.netflix.conductor.contribs.queue.amqp.config.AMQPEventQueueProperties;
import com.netflix.conductor.contribs.queue.amqp.config.AMQPRetryPattern;
import com.netflix.conductor.contribs.queue.amqp.util.AMQPSettings;
import com.netflix.conductor.contribs.queue.amqp.util.RetryType;
import com.netflix.conductor.core.events.queue.Message;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.PROTOCOL;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"rawtypes", "unchecked"})
@FixMethodOrder
public class AMQPObservableQueueTest {

    final int batchSize = 10;
    final int pollTimeMs = 500;

    Address[] addresses;
    AMQPEventQueueProperties properties;
    // Mock channel, connection and connectionFactory
    public Channel channel = Mockito.mock(Channel.class);
    public Connection connection = Mockito.mock(Connection.class);
    public ConnectionFactory connectionFactory = Mockito.mock(ConnectionFactory.class);

    @Before
    public void setUp() {
        properties = mock(AMQPEventQueueProperties.class);
        Mockito.when(properties.getBatchSize()).thenReturn(1);
        Mockito.when(properties.getPollTimeDuration()).thenReturn(Duration.ofMillis(100));
        Mockito.when(properties.getHosts()).thenReturn(ConnectionFactory.DEFAULT_HOST);
        Mockito.when(properties.getUsername()).thenReturn(ConnectionFactory.DEFAULT_USER);
        Mockito.when(properties.getPassword()).thenReturn(ConnectionFactory.DEFAULT_PASS);
        Mockito.when(properties.getVirtualHost()).thenReturn(ConnectionFactory.DEFAULT_VHOST);
        Mockito.when(properties.getPort()).thenReturn(PROTOCOL.PORT);
        Mockito.when(properties.getConnectionTimeoutInMilliSecs()).thenReturn(60000);
        Mockito.when(properties.isUseNio()).thenReturn(false);
        Mockito.when(properties.isDurable()).thenReturn(true);
        Mockito.when(properties.isExclusive()).thenReturn(false);
        Mockito.when(properties.isAutoDelete()).thenReturn(false);
        Mockito.when(properties.getContentType()).thenReturn("application/json");
        Mockito.when(properties.getContentEncoding()).thenReturn("UTF-8");
        Mockito.when(properties.getExchangeType()).thenReturn("topic");
        Mockito.when(properties.getDeliveryMode()).thenReturn(2);
        Mockito.when(properties.isUseExchange()).thenReturn(true);
        addresses = new Address[] {new Address("localhost", PROTOCOL.PORT)};
        AMQPConnection.setAMQPConnection(null);
        Mockito.reset(connection, channel, connectionFactory);
    }

    List<GetResponse> buildQueue(final Random random, final int bound) {
        final LinkedList<GetResponse> queue = new LinkedList();
        for (int i = 0; i < bound; i++) {
            AMQP.BasicProperties props = mock(AMQP.BasicProperties.class);
            Mockito.when(props.getMessageId()).thenReturn(UUID.randomUUID().toString());
            Envelope envelope = mock(Envelope.class);
            Mockito.when(envelope.getDeliveryTag()).thenReturn(random.nextLong());
            GetResponse response = mock(GetResponse.class);
            Mockito.when(response.getProps()).thenReturn(props);
            Mockito.when(response.getEnvelope()).thenReturn(envelope);
            Mockito.when(response.getBody()).thenReturn("{}".getBytes());
            Mockito.when(response.getMessageCount()).thenReturn(bound - i);
            queue.add(response);
        }
        return queue;
    }

    private AMQPObservableQueue getAMQPObservableInstance(
            ConnectionFactory connectionFactory, boolean useExchange) {
        final String name = RandomStringUtils.randomAlphabetic(30),
                type = "topic",
                routingKey = RandomStringUtils.randomAlphabetic(30);
        AMQPRetryPattern retrySettings = new AMQPRetryPattern(1, 10, RetryType.REGULARINTERVALS);
        final AMQPSettings settings =
                new AMQPSettings(properties)
                        .fromURI(
                                "amqp_exchange:"
                                        + name
                                        + "?exchangeType="
                                        + type
                                        + "&routingKey="
                                        + routingKey);
        return new AMQPObservableQueue(
                connectionFactory,
                addresses,
                useExchange,
                settings,
                retrySettings,
                batchSize,
                pollTimeMs);
    }

    private List<Message> getMsgs() {
        List<Message> messages = new LinkedList<>();
        Message msg = new Message();
        msg.setId("0e3eef8f-ebb1-4244-9665-759ab5bdf433");
        msg.setPayload("Payload");
        msg.setReceipt("1");
        messages.add(msg);
        return messages;
    }

    @Test(expected = RuntimeException.class)
    public void testAck_OnFailure() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, false);
        Mockito.when(channel.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.when(connection.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.doThrow(new IOException("Can't ack")).when(channel).basicAck(anyLong(), eq(false));
        observableQueue.ack(getMsgs());
        Mockito.verify(channel, Mockito.times(1)).basicAck(Mockito.any(), Mockito.any());
    }

    @Test
    public void testAck() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, false);
        Mockito.doNothing().when(channel).basicAck(anyLong(), eq(false));
        Mockito.when(channel.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.when(connection.isOpen()).thenReturn(Boolean.TRUE);
        List<String> deliveredTags = observableQueue.ack(getMsgs());
        assertNotNull(deliveredTags);
    }

    @Test
    public void testPollTime() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, true);
        observableQueue.setPollTimeInMS(2);
        assertEquals(observableQueue.getPollTimeInMS(), 2);
    }

    @Test
    public void testSize() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, true);
        Mockito.when(channel.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.when(connection.isOpen()).thenReturn(Boolean.TRUE);
        // on exception
        try {
            Mockito.doThrow(IOException.class).when(channel).messageCount(Mockito.any());
            observableQueue.size();
        } catch (Exception e) {
        }
        Mockito.verify(channel, Mockito.times(1)).messageCount(Mockito.any());
        long msgCount = 2;
        Mockito.doReturn(msgCount).when(channel).messageCount(Mockito.any());
        assertEquals(observableQueue.size(), msgCount);
    }

    // publish messages on proper connection
    @Test
    public void testPublish() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, true);
        Mockito.doNothing()
                .when(channel)
                .basicPublish(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.when(channel.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.when(connection.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.doReturn(null).when(channel).exchangeDeclarePassive(Mockito.any());
        // #1, proper publish
        observableQueue.publish(getMsgs());
        Mockito.verify(channel, Mockito.times(1))
                .basicPublish(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    // publish messages on improper connection
    @Test
    public void testPublish_OnChannelFailures() throws IOException, TimeoutException {
        Mockito.when(connection.createChannel()).thenReturn(channel);
        Mockito.when(connectionFactory.newConnection(eq(addresses), Mockito.anyString()))
                .thenReturn(connection);
        AMQPObservableQueue observableQueue = getAMQPObservableInstance(connectionFactory, true);
        Mockito.when(channel.isOpen()).thenReturn(Boolean.TRUE);
        Mockito.when(connection.isOpen()).thenReturn(Boolean.TRUE);
        try {
            Mockito.doThrow(IOException.class)
                    .when(channel)
                    .basicPublish(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
            Mockito.doReturn(null).when(channel).exchangeDeclarePassive(Mockito.any());
            observableQueue.publish(getMsgs());
        } catch (Exception e) {
        }
        Mockito.verify(channel, Mockito.times(2))
                .basicPublish(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAMQPObservalbleQueue_empty() throws IOException, TimeoutException {
        AMQPSettings settings = new AMQPSettings(properties).fromURI("amqp_queue:test");
        new AMQPObservableQueue(null, addresses, false, settings, null, batchSize, pollTimeMs);
    }

    // Initialize AMQPObservable Queue with empty addresses
    @Test(expected = IllegalArgumentException.class)
    public void testAMQPObservalbleQueue_addressNotNull() throws IOException, TimeoutException {
        AMQPSettings settings = new AMQPSettings(properties).fromURI("amqp_queue:test");
        Address[] addresses = new Address[0];
        new AMQPObservableQueue(
                connectionFactory, addresses, false, settings, null, batchSize, pollTimeMs);
    }

    // Initialize Builder with proper details
    @Test
    public void testBuilder() {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        AMQPObservableQueue amqp = new Builder(aqmpProps).build(true, "amqp_queue:test");
        aqmpProps.setUseNio(true);
        assertNotNull(amqp);
    }

    // Initialize Builder with proper details
    @Test
    public void testBuilder_withExchangeAsFalse() {
        AMQPObservableQueue amqp = new Builder(properties).build(false, "amqp_queue:test");
        assertNotNull(amqp);
    }

    // Initialize Builder with improper details
    @Test(expected = IllegalArgumentException.class)
    public void testBuilder_withEmptyHosts() throws IllegalArgumentException {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        aqmpProps.setHosts("");
        new Builder(aqmpProps).build(false, "amqp_queue:test");
    }

    // Initialize Builder with improper details
    @Test(expected = IllegalArgumentException.class)
    public void testBuilder_withEmptyVirtualHost() throws IllegalArgumentException {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        aqmpProps.setVirtualHost("");
        new Builder(aqmpProps).build(false, "amqp_queue:test");
    }

    // Initialize Builder with improper details
    @Test(expected = IllegalArgumentException.class)
    public void testBuilder_withEmptyUsername() throws IllegalArgumentException {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        aqmpProps.setUsername("");
        new Builder(aqmpProps).build(false, "amqp_queue:test");
    }

    // Initialize Builder with improper details
    @Test(expected = IllegalArgumentException.class)
    public void testBuilder_withEmptyPassword() throws IllegalArgumentException {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        aqmpProps.setPassword("");
        new Builder(aqmpProps).build(false, "amqp_queue:test");
    }

    // Initialize Builder with improper details
    @Test(expected = IllegalArgumentException.class)
    public void testBuilder_withInvalidPort() throws IllegalArgumentException {
        AMQPEventQueueProperties aqmpProps = new AMQPEventQueueProperties();
        aqmpProps.setPort(-1);
        new Builder(aqmpProps).build(false, "amqp_queue:test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAMQPObservalbleQueue_settingsEmpty() throws IOException, TimeoutException {
        new AMQPObservableQueue(
                connectionFactory, addresses, false, null, null, batchSize, pollTimeMs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAMQPObservalbleQueue_batchsizezero() throws IOException, TimeoutException {
        AMQPSettings settings = new AMQPSettings(properties).fromURI("amqp_queue:test");
        new AMQPObservableQueue(connectionFactory, addresses, false, settings, null, 0, pollTimeMs);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAMQPObservalbleQueue_polltimezero() throws IOException, TimeoutException {
        AMQPSettings settings = new AMQPSettings(properties).fromURI("amqp_queue:test");
        new AMQPObservableQueue(connectionFactory, addresses, false, settings, null, batchSize, 0);
    }
}
