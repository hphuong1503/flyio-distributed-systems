package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	n         *maelstrom.Node
	mu        sync.RWMutex
	messages  map[int]struct{}
	neighbors []string
}

type GossipBody struct {
	Type    string `json:"type"`
	Message []int  `json:"message"`
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
	}
	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("gossip", s.handleGossip)

	go s.gossiploop()

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	MsgID   int    `json:"msg_id,omitempty"`
}

func (s *server) gossiploop() {
	// create a timer that will trigger after 500 milliseconds
	ticker := time.NewTicker(50 * time.Millisecond)

	for range ticker.C {
		// forward the message to all neighbors
		s.mu.RLock()
		if len(s.messages) == 0 {
			s.mu.RUnlock()
			continue
		}

		// convert the map keys to a slice
		messages := make([]int, 0, len(s.messages))
		for message := range s.messages {
			messages = append(messages, message)
		}

		// make a copy of the neighbors slice to avoid race conditions
		neighbors := make([]string, len(s.neighbors))
		copy(neighbors, s.neighbors)
		s.mu.RUnlock()

		// create a gossip message with the current messages
		gossipMsg := GossipBody{
			Type:    "gossip",
			Message: messages,
		}

		// send the gossip message to all neighbors
		for _, neighbor := range neighbors {
			if err := s.n.Send(neighbor, gossipMsg); err != nil {
				log.Printf("Failed to send message to neighbor %s: %v", neighbor, err)
			}
		}
	}

}

func (s *server) handleGossip(msg maelstrom.Message) error {
	var body GossipBody

	// check if the message is valid
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// lock the mutex for writing
	s.mu.Lock()

	// add the received messages to the map
	for _, message := range body.Message {
		s.messages[message] = struct{}{}
	}
	s.mu.Unlock()

	return nil
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	var body BroadcastBody

	//check if the message is valid
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// lock the mutex for writing
	s.mu.Lock()

	// check if the message has already been received
	if _, exists := s.messages[body.Message]; exists {
		s.mu.Unlock()
		if body.MsgID != 0 {
			return s.n.Reply(msg, map[string]any{
				"type": "broadcast_ok",
			})
		}
		return nil
	}

	//if the message is new, add it to the map
	s.messages[body.Message] = struct{}{}
	s.mu.Unlock()

	// forward the message to all neighbors except the source
	for _, neighbor := range s.neighbors {
		if neighbor != msg.Src {
			// send the message to the neighbor
			if err := s.n.Send(neighbor, body); err != nil {
				log.Printf("Failed to send message to neighbor %s: %v", neighbor, err)
			}
		}
	}

	// send the response back to the client
	if body.MsgID != 0 {
		response := map[string]any{
			"type": "broadcast_ok",
		}
		return s.n.Reply(msg, response)
	}

	return nil
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

	var body struct {
		Type     string              `json:"type"`
		Topology map[string][]string `json:"topology"`
	}

	// unmarshal the message body into the struct
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	s.neighbors = body.Topology[s.n.ID()]
	s.mu.Unlock()

	// send the response back to the client
	response := map[string]any{
		"type": "topology_ok",
	}
	return s.n.Reply(msg, response)
}
