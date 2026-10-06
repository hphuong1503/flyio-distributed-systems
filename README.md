# Fly.io Distributed Systems Challenges (Maelstrom)

Solutions for the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/) implemented in Go using [Maelstrom](https://github.com/jepsen-io/maelstrom).

## Challenges Progress & Status

| # | Challenge | Directory | Status | Key Highlights |
|---|---|---|:---:|---|
| 1 | [Echo](https://fly.io/dist-sys/1/) | [`maelstrom-echo`](./maelstrom-echo) | ✅ **Completed** | Single-node baseline sanity check |
| 2 | [Unique ID Generation](https://fly.io/dist-sys/2/) | [`maelstrom-unique-ids`](./maelstrom-unique-ids) | ✅ **Completed** | Snowflake 64-bit & Atomic Counter (1,000 req/s, 0 duplicates) |
| 3a | [Single-Node Broadcast](https://fly.io/dist-sys/3a/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | ✅ **Completed** | Thread-safe in-memory store with `sync.RWMutex` |
| 3b | [Multi-Node Broadcast](https://fly.io/dist-sys/3b/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | ✅ **Completed** | Multi-hop routing & broadcast loop termination |
| 3c | [Fault-Tolerant Broadcast](https://fly.io/dist-sys/3c/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | ✅ **Completed** | Periodic anti-entropy gossip resilient to network partitions |
| 3d | [Efficient Broadcast I](https://fly.io/dist-sys/3d/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | ✅ **Completed** | Star topology, 23.66 msgs/op (< 30), 180ms median latency (< 400ms) |
| 3e | [Efficient Broadcast II](https://fly.io/dist-sys/3e/) | [`maelstrom-broadcast`](./maelstrom-broadcast) | ✅ **Completed** | Balanced tree + Delta Tracking, 8.49 msgs/op (< 20) under partition |
| 4 | [Grow-Only Counter](https://fly.io/dist-sys/4/) | `maelstrom-counter` | 🚧 **In Progress** | G-Counter CRDT backed by `seq-kv` & Nemesis Partition |
| 5 | [Kafka-Style Log](https://fly.io/dist-sys/5a/) | - | ⏳ Pending | Single-node, multi-node & replicated log |
| 6 | [Totally-Ordered Transactions](https://fly.io/dist-sys/6a/) | - | ⏳ Pending | Distributed transactional KV |

## Detailed Test Statistics & Benchmarks

### 📊 Challenge 1: Echo
* **Node Count:** 1
* **Result:** `:valid? true`, `:ok-fraction 1.0`

### 📊 Challenge 2: Unique ID Generation
* **Workload & Command:** `maelstrom test -w unique-ids --node-count 3 --time-limit 30 --rate 1000 --availability total --nemesis partition`
* **Throughput:** **1,000 requests/sec** (25,000 total operations)
* **Duplication:** `:duplicated-count 0` (Zero duplicates)
* **Availability:** `:ok-fraction 1.0` (100% total availability under network partition)
* **Implementations:**
  * Node ID + Local Atomic Counter (Coordination-free)
  * Twitter Snowflake 64-bit (Time-sortable, handles clock drift & avoids JSON 64-bit float truncation)

### 📊 Challenge 3: Broadcast (Comprehensive Optimization)
* **Challenge 3a & 3b:** Single-node baseline & 5-node cluster consistency, broadcast storm termination.
* **Challenge 3c (Fault-Tolerant):** Network partition resilience (5 nodes, rate 10), `:lost-count 0`, `:never-read ()`.
* **Challenge 3d (Efficient I - 25 nodes, latency 100ms, rate 100 req/s):**
  * **Messages per op:** `23.66` (Target: $< 30$) — Exceeded goal
  * **Median latency:** `180 ms` (Target: $< 400\text{ ms}$) — Exceeded goal
  * **Max latency:** `345 ms` (Target: $< 600\text{ ms}$) — Exceeded goal
  * **Topology:** Star Topology (2-tier hub-and-spoke)
* **Challenge 3e (Efficient II - 25 nodes, rate 100 req/s, latency 100ms + Nemesis Partition):**
  * **Validation:** `:valid? true`
  * **Messages per op (Servers):** **`8.49`** (Target: $< 20$) — Highly optimized (~42% of budget)
  * **Messages per op (Total):** `10.55` (Target: $< 20$)
  * **Median latency:** `747 ms` (Target: $< 1000\text{ ms}$)
  * **Lost count:** `0`, **Availability:** `1.0` (1,739/1,739 operations)
  * **Architecture:** 3-tier Balanced Spanning Tree + Delta Tracking (`unAcked`) Buffer + Periodic Batched Gossip (100ms)

---

## 🚧 Current Work: Challenge 4 - Grow-Only Counter (G-Counter)

* **Objective:** Implement a stateless Grow-Only Counter (G-Counter CRDT) backed by Maelstrom's sequential-consistency key-value store (`seq-kv`).
* **Environment:** 3 nodes, rate 100 req/s, time limit 20s, `--nemesis partition`.

---

## Prerequisites

- Go 1.21+
- Java (JRE/JDK 11+)
- [Maelstrom](https://github.com/jepsen-io/maelstrom/releases)
