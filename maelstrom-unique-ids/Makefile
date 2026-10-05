.PHONY: all build build-simple build-snowflake clean test-simple test-snowflake

BIN_DIR := bin
SIMPLE_BIN := $(BIN_DIR)/maelstrom-simple
SNOWFLAKE_BIN := $(BIN_DIR)/maelstrom-snowflake

all: build

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

build-simple: $(BIN_DIR)
	go build -o $(SIMPLE_BIN) ./cmd/simple

build-snowflake: $(BIN_DIR)
	go build -o $(SNOWFLAKE_BIN) ./cmd/snowflake

build: build-simple build-snowflake

clean:
	rm -rf $(BIN_DIR)
