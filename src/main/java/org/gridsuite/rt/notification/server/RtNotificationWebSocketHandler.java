/**
 * Copyright (c) 2026, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.rt.notification.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * A WebSocketHandler that forwards the execution notifications received from the broker to the websockets
 * opened by clients on this instance, interleaving with pings to keep connections open.
 * <p>
 * Spring Cloud Stream gets the executionNotifications bean and calls it with the flux from the broker.
 * Each message is emitted into a multicast sink that is subscribed by every connected websocket and
 * that discards the messages when no websocket is connected.
 * <p>
 * Every websocket has its own bounded buffer: a slow or broken client never slows down the others.
 * When its buffer overflows, the websocket is closed so that the client reconnects and reloads its state.
 *
 * @author Mohamed Ben Rejeb <mohamed.benrejeb at rte-france.com>
 */
@Component
public class RtNotificationWebSocketHandler implements WebSocketHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RtNotificationWebSocketHandler.class);

    private static final String CATEGORY_BROKER_INPUT = RtNotificationWebSocketHandler.class.getName() + ".messages.input-broker";
    private static final String CATEGORY_WS_OUTPUT = RtNotificationWebSocketHandler.class.getName() + ".messages.output-websocket";

    static final String HEADER_UPDATE_TYPE = "updateType";
    static final String HEADER_EXECUTION_ID = "executionId";

    private final ObjectMapper jacksonObjectMapper;

    private final int heartbeatInterval;

    private final int sessionBufferSize;

    private final Sinks.Many<Message<String>> sink = Sinks.many().multicast().directBestEffort();

    public RtNotificationWebSocketHandler(ObjectMapper jacksonObjectMapper,
                                                @Value("${notification.websocket.heartbeat.interval:30}") int heartbeatInterval,
                                                @Value("${notification.websocket.session-buffer-size:1000}") int sessionBufferSize) {
        this.jacksonObjectMapper = jacksonObjectMapper;
        this.heartbeatInterval = heartbeatInterval;
        this.sessionBufferSize = sessionBufferSize;
    }

    @Bean
    public Consumer<Flux<Message<String>>> executionNotifications() {
        return f -> f.log(CATEGORY_BROKER_INPUT, Level.FINE)
                .subscribe(this::emit, e -> LOGGER.error("Execution notifications flux terminated in error", e));
    }

    private void emit(Message<String> message) {
        Sinks.EmitResult result = sink.tryEmitNext(message);
        if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            LOGGER.warn("Execution notification not forwarded to websockets: {}", result);
        }
    }

    /**
     * map from the broker flux to the flux for one websocket client, extracting only relevant headers.
     */
    private Flux<WebSocketMessage> notificationFlux(WebSocketSession webSocketSession) {
        return sink.asFlux()
                .filter(RtNotificationWebSocketHandler::isValid)
                .map(this::toJson)
                .onBackpressureBuffer(sessionBufferSize, dropped -> closeSlowSession(webSocketSession))
                .log(CATEGORY_WS_OUTPUT, Level.FINE)
                .map(webSocketSession::textMessage);
    }

    private static void closeSlowSession(WebSocketSession webSocketSession) {
        LOGGER.warn("Websocket session {} is too slow, closing it", webSocketSession.getId());
        webSocketSession.close(CloseStatus.SERVICE_OVERLOAD).subscribe();
    }

    private static boolean isValid(Message<String> message) {
        MessageHeaders headers = message.getHeaders();
        if (headers.get(HEADER_UPDATE_TYPE) == null || headers.get(HEADER_EXECUTION_ID) == null) {
            LOGGER.warn("Ignoring execution notification without '{}' or '{}' header: {}", HEADER_UPDATE_TYPE, HEADER_EXECUTION_ID, headers);
            return false;
        }
        return true;
    }

    private String toJson(Message<String> message) {
        MessageHeaders headers = message.getHeaders();
        try {
            return jacksonObjectMapper.writeValueAsString(Map.of(
                    "headers", Map.of(
                            HEADER_UPDATE_TYPE, Objects.toString(headers.get(HEADER_UPDATE_TYPE)),
                            HEADER_EXECUTION_ID, Objects.toString(headers.get(HEADER_EXECUTION_ID)))));
        } catch (JsonProcessingException e) {
            throw new RtNotificationServerRuntimeException("Unable to serialize execution notification", e);
        }
    }

    /**
     * A heartbeat flux sending websockets pings
     */
    private Flux<WebSocketMessage> heartbeatFlux(WebSocketSession webSocketSession) {
        return Flux.interval(Duration.ofSeconds(heartbeatInterval)).map(n -> webSocketSession
                .pingMessage(dbf -> dbf.wrap((webSocketSession.getId() + "-" + n).getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    public Mono<Void> handle(WebSocketSession webSocketSession) {
        return webSocketSession
                .send(notificationFlux(webSocketSession).mergeWith(heartbeatFlux(webSocketSession)))
                .and(webSocketSession.receive());
    }
}
