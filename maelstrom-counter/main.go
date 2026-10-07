package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// server implements stateful write-behind CRDT 
type server struct {
	n            *maelstrom.Node
	kv           *maelstrom.KV
	mu           sync.RWMutex
	pendingDelta int            
	cache        map[string]int 
}

func main() {
	n := maelstrom.NewNode()
	s := &server{
		n:     n,
		kv:    maelstrom.NewSeqKV(n),
		cache: make(map[string]int),
	}


	go s.flushAndSyncLoop()

	s.n.Handle("add", s.handleAdd)
	s.n.Handle("read", s.handleRead)

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}

func (s *server) handleAdd(msg maelstrom.Message) error {
	var body struct {
		Type  string `json:"type"`
		Delta int    `json:"delta"`
	}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.pendingDelta += body.Delta
	s.cache[s.n.ID()] += body.Delta 
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

func (s *server) handleRead(msg maelstrom.Message) error {
	s.mu.RLock()
	total := 0
	for _, v := range s.cache {
		total += v
	}
	s.mu.RUnlock()

	return s.n.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": total,
	})
}

func (s *server) flushAndSyncLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		s.flushPending()
		s.syncPeers()
	}
}

func (s *server) flushPending() {
	s.mu.Lock()
	delta := s.pendingDelta
	s.mu.Unlock()

	if delta <= 0 {
		return
	}

	key := s.n.ID()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	val, err := s.kv.ReadInt(ctx, key)
	if err != nil {
		var rpcErr *maelstrom.RPCError
		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
			// Key does not exist: create with delta
			casErr := s.kv.CompareAndSwap(ctx, key, 0, delta, true)
			if casErr == nil {
				s.mu.Lock()
				s.pendingDelta -= delta
				s.mu.Unlock()
			}
			return
		}
		return
	}

	// Key exists: CAS val -> val + delta
	casErr := s.kv.CompareAndSwap(ctx, key, val, val+delta, false)
	if casErr == nil {
		s.mu.Lock()
		s.pendingDelta -= delta
		s.mu.Unlock()
	}
}

func (s *server) syncPeers() {
	nodes := s.n.NodeIDs()
	if len(nodes) == 0 {
		return
	}

	for _, nid := range nodes {
		if nid == s.n.ID() {
			continue // Skip own key; managed locally in RAM
		}

		go func(id string) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			val, err := s.kv.ReadInt(ctx, id)
			if err == nil {
				s.mu.Lock()
				if val > s.cache[id] {
					s.cache[id] = val 
				}
				s.mu.Unlock()
			}
		}(nid)
	}
}
