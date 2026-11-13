# Real Time Notification Server

[![Actions Status](https://github.com/gridsuite/rt-notification-server/workflows/CI/badge.svg)](https://github.com/gridsuite/rt-notification-server/actions)
[![Coverage Status](https://sonarcloud.io/api/project_badges/measure?project=org.gridsuite%3Art-notification-server&metric=coverage)](https://sonarcloud.io/component_measures?id=org.gridsuite%3Art-notification-server&metric=coverage)
[![MPL-2.0 License](https://img.shields.io/badge/license-MPL_2.0-blue.svg)](https://www.mozilla.org/en-US/MPL/2.0/)

## Description
Real Time Notification Server is the GridRT notification service. It consumes process execution notifications from RabbitMQ and exposes them to GridRT clients through a WebSocket endpoint.

The notifications only tell that an execution must be reloaded: the HTTP endpoints of the snapshot refiner server remain the source of truth.

## Functional Scope

- Consume execution notifications from the `execution-notifications` RabbitMQ destination.
- The input binding has no consumer group: each instance gets its own anonymous queue, so every instance receives every notification and forwards it to the websockets connected to it.
- Broadcast notifications to all connected WebSocket clients. Each client has its own bounded buffer: a slow or broken client is closed (status `1013`) without slowing down the others, and is expected to reconnect and reload its state.
- Send periodic WebSocket ping frames to keep client connections alive.

## Technical Stack

- Spring Boot (WebFlux, Actuator, Cloud Stream)
- RabbitMQ via Spring Cloud Stream
- WebSocket

## Development Scripts

Build Docker image

```shell
mvn install -DskipTests -Dpowsybl.docker.install
```

## RabbitMQ messages

Messages have an empty body and two headers:

| Header        | Value                                                            |
|---------------|------------------------------------------------------------------|
| `updateType`  | `EXECUTION_CREATED` \| `EXECUTION_UPDATED` \| `EXECUTION_DELETED` |
| `executionId` | identifier of the execution                                      |

Messages missing one of these headers are ignored.

## WebSocket API

The service exposes one WebSocket endpoint:

```text
/notify
```

Each outbound text message is a JSON object with the notification headers:

```json
{
  "headers": {
    "updateType": "EXECUTION_UPDATED",
    "executionId": "..."
  }
}
```

## Configuration

| Property                                     | Default | Description                                                         |
|----------------------------------------------|---------|---------------------------------------------------------------------|
| `notification.websocket.heartbeat.interval`  | `30`    | Interval in seconds between two websocket pings                     |
| `notification.websocket.session-buffer-size` | `1000`  | Notifications buffered per websocket before closing a slow session |
