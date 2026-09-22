# mod-telemetry-redis

An [AzerothCore](https://www.azerothcore.org/) module for the
[Playerbots](https://github.com/liyunfan1223/mod-playerbots) fork (WotLK 3.3.5a) that
streams playerbot telemetry to a **Redis stream**.

## What it does

Emits telemetry events to a Redis stream (`XADD`) via `hiredis` on a dedicated background
worker thread. If Redis is unavailable the world is never affected: events are buffered in a
bounded in-memory ring and dropped when the buffer is full or there is no connection.
Reconnects happen lazily with backoff.

Telemetry is active only when **both** `DadMode.Enabled` (owned by
[mod-bot-economy](https://github.com/maluramichael/mod-bot-economy)) and
`DadMode.Telemetry.Enabled` are `1`.

## Requirements

- The Playerbots fork of AzerothCore.
- A reachable Redis instance (built with `hiredis`).

## Configuration

`conf/mod_telemetry_redis.conf.dist`: `DadMode.Telemetry.Enabled` and the Redis
host/port/stream settings.

## Installation

Clone into your AzerothCore `modules/` directory and rebuild the worldserver.

## License

Released under the GNU GPL v2 (or later).
