package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Bit allocation constants for Snowflake
const (
	epoch        = int64(1704067200000) // Custom Epoch: 2024-01-01 00:00:00 UTC
	nodeBits     = uint(10)             // 10 bits for Node ID (up to 1024 nodes)
	sequenceBits = uint(12)             // 12 bits for Sequence (up to 4096 IDs/ms)

	maxNodeID   = int64(-1 ^ (-1 << nodeBits))     // 1023
	maxSequence = int64(-1 ^ (-1 << sequenceBits)) // 4095

	timeShift = nodeBits + sequenceBits // Shift left by 22 bits
	nodeShift = sequenceBits            // Shift left by 12 bits
)

// Snowflake manages the state of the ID generator on a node
type Snowflake struct {
	mu            sync.Mutex
	nodeID        int64
	lastTimestamp int64
	sequence      int64
}

func NewSnowflake(nodeID int64) *Snowflake {
	if nodeID < 0 || nodeID > maxNodeID {
		log.Fatalf("Node ID must be between 0 and %d", maxNodeID)
	}
	return &Snowflake{nodeID: nodeID}
}

// Generate creates a unique 64-bit ID
func (s *Snowflake) Generate() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()

	// Handle Clock Drift (system clock moving backwards)
	// Wait until system clock catches up to lastTimestamp
	if now < s.lastTimestamp {
		for now < s.lastTimestamp {
			time.Sleep(time.Duration(s.lastTimestamp-now) * time.Millisecond)
			now = time.Now().UnixMilli()
		}
	}

	// Handle requests arriving within the same millisecond
	if now == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & maxSequence
		// Sequence exhausted in the current millisecond -> Wait for next millisecond
		if s.sequence == 0 {
			for now <= s.lastTimestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		// New millisecond reached -> Reset sequence to 0
		s.sequence = 0
	}

	s.lastTimestamp = now

	// Combine bit segments into a single 64-bit unique integer
	id := ((now - epoch) << timeShift) |
		(s.nodeID << nodeShift) |
		s.sequence

	return id
}

func main() {
	n := maelstrom.NewNode()

	var sf *Snowflake
	var initOnce sync.Once

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Lazily initialize Snowflake once the node ID (e.g., n0, n1, n2...) is known
		initOnce.Do(func() {
			// Parse numeric ID from node name (e.g., "n1" -> 1, "n2" -> 2)
			rawID := strings.TrimPrefix(n.ID(), "n")
			idNum, err := strconv.ParseInt(rawID, 10, 64)
			if err != nil {
				idNum = 0
			}
			sf = NewSnowflake(idNum)
		})

		// Generate a 64-bit ID
		uniqueID := sf.Generate()

		// CRITICAL: Maelstrom Go SDK internally unmarshals reply bodies into map[string]any,
		// which converts JSON numbers to float64. Since float64 only has 53 bits of precision,
		// a 64-bit integer will lose its lower bits (sequence numbers), causing duplicate IDs!
		// Returning the 64-bit Snowflake ID as a string prevents float64 truncation (identical to Twitter id_str).
		return n.Reply(msg, map[string]any{
			"type": "generate_ok",
			"id":   strconv.FormatInt(uniqueID, 10),
		})
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
