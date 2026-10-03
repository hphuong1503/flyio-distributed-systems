package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n        *maelstrom.Node
	mu       sync.RWMutex
	messages map[int]struct{}
	neighbors []string
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}
	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastBody

	//check if the message is valid
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// lock the mutex for writing
	s.mu.Lock()
	s.messages[body.Message] = struct{}{}
	s.mu.Unlock()

	// send the response back to the client
	response := map[string]any{
		"type": "broadcast_ok",
	}
	return s.n.Reply(msg, response)
}

func (s *server) handleRead(msg maelstrom.Message) error {
	// lock the mutex for reading
	s.mu.RLock()

	// convert the map keys to a slice
	messages := make([]int, 0, len(s.messages))
	for message := range s.messages {
		messages = append(messages, message)
	}

	// send the response back to the client
	response := map[string]any{
		"type":     "read_ok",
		"messages": messages,
	}
	s.mu.RUnlock()
	return s.n.Reply(msg, response)

}

func (s *server) handleTopology(msg maelstrom.Message) error {
	// send the response back to the client
	response := map[string]any{
		"type": "topology_ok",
	}
	return s.n.Reply(msg, response)
}
