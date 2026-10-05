# Build & Run Guide: Maelstrom Unique IDs

## 1. Project Structure
- `cmd/simple/main.go`: Node-ID + Atomic counter generator.
- `cmd/snowflake/main.go`: 64-bit Twitter Snowflake generator.

## 2. Build

```bash
mkdir -p bin
go build -o bin/maelstrom-simple ./cmd/simple
go build -o bin/maelstrom-snowflake ./cmd/snowflake
```
*(Or run `make build`)*

## 3. Test with Maelstrom

### Simple ID
```bash
./maelstrom test -w unique-ids --bin ./bin/maelstrom-simple \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability-total --nemesis partition
```

### Snowflake ID
```bash
./maelstrom test -w unique-ids --bin ./bin/maelstrom-snowflake \
  --time-limit 30 --rate 1000 --node-count 3 \
  --availability-total --nemesis partition
```

## 4. Key Notes
- **Zero Coordination**: Both algorithms run locally without RPC, surviving network partitions (`--nemesis partition`) with 100% availability.
- **Snowflake String ID**: IDs are sent as strings to prevent Maelstrom's JSON unmarshaler from truncating 64-bit integers into `float64` (53-bit precision).
