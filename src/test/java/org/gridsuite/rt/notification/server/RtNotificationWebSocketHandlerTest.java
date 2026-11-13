/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.rt.notification.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.gridsuite.rt.notification.server.RtNotificationWebSocketHandler.HEADER_EXECUTION_ID;
import static org.gridsuite.rt.notification.server.RtNotificationWebSocketHandler.HEADER_UPDATE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Mohamed Ben Rejeb <mohamed.benrejeb at rte-france.com>
 */
class RtNotificationWebSocketHandlerTest {

    private static final String EMPTY_PAYLOAD = "";

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

    private WebSocketSession mockSession(String id) {
        WebSocketSession webSocketSession = mock(WebSocketSession.class);
        when(webSocketSession.getId()).thenReturn(id);
        when(webSocketSession.receive()).thenReturn(Flux.empty());
        when(webSocketSession.send(any())).thenReturn(Mono.empty());
        when(webSocketSession.textMessage(any())).thenAnswer(invocation ->
            new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap(invocation.getArgument(0, String.class).getBytes(StandardCharsets.UTF_8)))
        );
        when(webSocketSession.pingMessage(any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Function<DataBufferFactory, DataBuffer> payloadFactory = invocation.getArgument(0, Function.class);
            return new WebSocketMessage(WebSocketMessage.Type.PING, payloadFactory.apply(dataBufferFactory));
        });
        return webSocketSession;
    }

    private static Message<String> notification(String updateType, String executionId) {
        return new GenericMessage<>(EMPTY_PAYLOAD, Map.of(HEADER_UPDATE_TYPE, updateType, HEADER_EXECUTION_ID, executionId, "ignored", "value"));
    }

    private static FluxSink<Message<String>> connectBroker(RtNotificationWebSocketHandler handler) {
        AtomicReference<FluxSink<Message<String>>> sinkReference = new AtomicReference<>();
        handler.executionNotifications().accept(Flux.create(sinkReference::set));
        return sinkReference.get();
    }

    @Test
    void testShouldSerializeNotificationWithSupportedHeadersOnly() throws Exception {
        RtNotificationWebSocketHandler handler = new RtNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE, 10);
        FluxSink<Message<String>> broker = connectBroker(handler);
        WebSocketSession session = mockSession("test-session");
        handler.handle(session);

        List<String> messages = new ArrayList<>();
        Disposable subscription = captureOutboundMessages(session).map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);

        broker.next(notification("EXECUTION_UPDATED", "exec-id"));

        assertEquals(1, messages.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = objectMapper.readValue(messages.get(0), Map.class);
        subscription.dispose();

        assertInstanceOf(Map.class, payload.get("headers"));
        assertEquals(Map.of(HEADER_UPDATE_TYPE, "EXECUTION_UPDATED", HEADER_EXECUTION_ID, "exec-id"), payload.get("headers"));
        assertEquals(1, payload.size());
    }

    @Test
    void testShouldEmitHeartbeatMessages() {
        RtNotificationWebSocketHandler handler = new RtNotificationWebSocketHandler(objectMapper, 1, 10);
        handler.executionNotifications().accept(Flux.empty());
        WebSocketSession session = mockSession("test-session");
        handler.handle(session);

        WebSocketMessage heartbeat = captureOutboundMessages(session).blockFirst(Duration.ofSeconds(2));
        assertEquals(WebSocketMessage.Type.PING, heartbeat.getType());
        assertEquals("test-session-0", heartbeat.getPayloadAsText());
    }

    @Test
    void testHandleShouldConsumeIncomingMessages() {
        WebSocketSession session = mockSession("test-session");
        WebSocketMessage textMessage = new WebSocketMessage(WebSocketMessage.Type.TEXT, dataBufferFactory.wrap("hello".getBytes(StandardCharsets.UTF_8)));
        WebSocketMessage binaryMessage = new WebSocketMessage(WebSocketMessage.Type.BINARY, dataBufferFactory.wrap(new byte[] {1, 2, 3}));
        List<WebSocketMessage> receivedMessages = new ArrayList<>();
        when(session.receive()).thenReturn(Flux.just(textMessage, binaryMessage).doOnNext(receivedMessages::add));

        RtNotificationWebSocketHandler handler = new RtNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE, 10);
        handler.executionNotifications().accept(Flux.empty());
        handler.handle(session).block(Duration.ofSeconds(1));

        assertEquals(2, receivedMessages.size());
        assertSame(textMessage, receivedMessages.get(0));
        assertSame(binaryMessage, receivedMessages.get(1));
    }

    @Test
    void testShouldNotReplayMessagesSentIfNoClientSubscribed() {
        RtNotificationWebSocketHandler handler = new RtNotificationWebSocketHandler(objectMapper, Integer.MAX_VALUE, 10);
        FluxSink<Message<String>> broker = connectBroker(handler);

        broker.next(notification("EXECUTION_CREATED", "discarded")); // discarded, no client connected

        WebSocketSession session = mockSession("test-session");
        handler.handle(session);
        List<String> messages = new ArrayList<>();
        Disposable subscription = captureOutboundMessages(session).map(WebSocketMessage::getPayloadAsText).subscribe(messages::add);

        broker.next(notification("EXECUTION_UPDATED", "delivered"));
        subscription.dispose();

        assertEquals(1, messages.size());
        assertTrue(messages.get(0).contains("delivered"));
    }

    @SuppressWarnings("unchecked")
    private static Flux<WebSocketMessage> captureOutboundMessages(WebSocketSession session) {
        ArgumentCaptor<Flux<WebSocketMessage>> argumentCaptor = ArgumentCaptor.forClass(Flux.class);
        verify(session).send(argumentCaptor.capture());
        return argumentCaptor.getValue();
    }
}
