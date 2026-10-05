# Fly.io Distributed Systems Challenges (Maelstrom)

Solutions for the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/) implemented in Go using [Maelstrom](https://github.com/jepsen-io/maelstrom).

## Challenges

| # | Challenge | Directory | Description |
|---|---|---|---|
| 1 | [Echo](https://fly.io/dist-sys/1/) | [`maelstrom-echo`](./maelstrom-echo) | Basic node echo response implementation |
| 2 | [Unique ID Generation](https://fly.io/dist-sys/2/) | [`maelstrom-unique-ids`](./maelstrom-unique-ids) | Globally unique ID generator (Simple counter & Snowflake 64-bit) |
| 3 | [Broadcast](https://fly.io/dist-sys/3a/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | Single-node, multi-node, fault-tolerant & efficient broadcast |

## Prerequisites

- Go 1.21+
- Java (JRE/JDK 11+)
- [Maelstrom](https://github.com/jepsen-io/maelstrom/releases)
