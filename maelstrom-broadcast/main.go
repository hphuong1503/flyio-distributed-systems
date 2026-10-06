package main

import (
	"encoding/json"
	"fmt"
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
	unAcked   map[string]map[int]struct{}
}

type GossipBody struct {
	Type    string `json:"type"`
	Message []int  `json:"message"`
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
	MsgID   int    `json:"msg_id,omitempty"`
}

func main() {
	s := &server{
		n:        maelstrom.NewNode(),
		messages: make(map[int]struct{}),
		unAcked:  make(map[string]map[int]struct{}),
	}
	s.n.Handle("broadcast", s.handleBroadcast)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("topology", s.handleTopology)
	s.n.Handle("gossip", s.handleGossip)

	go s.gossiploop()

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}



func (s *server) gossiploop() {
	// create a timer that will trigger after 100 milliseconds
	ticker := time.NewTicker(100 * time.Millisecond)

	for range ticker.C {
		s.mu.RLock()
		type batchItem struct {
			neighbor string
			message  []int
		}
		var batches []batchItem
		// batch the unAcked messages for each neighbor
		for _, neighbor := range s.neighbors {
			unAckSet := s.unAcked[neighbor]
			if len(unAckSet) == 0 {
				continue
			}
			msg := make([]int, 0, len(unAckSet))
			for message := range unAckSet {
				msg = append(msg, message)
			}
			batches = append(batches, batchItem{neighbor: neighbor, message: msg})
		}
		s.mu.RUnlock()

		//send batches to neighbors
		for _, batch := range batches {
			neighbor := batch.neighbor
			messages := batch.message

			// send the gossip message to the neighbor
			gossipMsg := GossipBody{
				Type:    "gossip",
				Message: messages,
			}

			s.n.RPC(neighbor, gossipMsg, func(reply maelstrom.Message) error {
				s.mu.Lock()
				// remove the acknowledged messages from the unAcked map for the neighbor
				if s.unAcked[neighbor] != nil {
					for _, message := range messages {
						delete(s.unAcked[neighbor], message)
					}
				}
				s.mu.Unlock()
				return nil
			})
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

	// populate the messages map with the received messages
	for _, message := range body.Message {
		if _, exists := s.messages[message]; !exists {
			s.messages[message] = struct{}{}
			// populate the unAcked map for each neighbor with the received messages
			for _, neighbor := range s.neighbors {
				if neighbor == msg.Src {
					continue // skip the neighbor that sent the gossip message
				}
				if s.unAcked[neighbor] == nil {
					s.unAcked[neighbor] = make(map[int]struct{})
				}
				s.unAcked[neighbor][message] = struct{}{}
			}
		}

	}

	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "gossip_ok",
	})
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
	if _, exists := s.messages[body.Message]; !exists {
		// add the message to the messages map
		s.messages[body.Message] = struct{}{}
		// add the message to the unAcked map for each neighbor
		for _, neighbor := range s.neighbors {
			if s.unAcked[neighbor] == nil {
				s.unAcked[neighbor] = make(map[int]struct{})
			}
			s.unAcked[neighbor][body.Message] = struct{}{}
		}
	}
	s.mu.Unlock()

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
	s.neighbors = buildBalanceTree(s.n.ID(), s.n.NodeIDs())

	// intialize the unAcked map for each neighbor
	for _, neighbor := range s.neighbors {
		if _, exists := s.unAcked[neighbor]; !exists {
			s.unAcked[neighbor] = make(map[int]struct{})
		}
		for message := range s.messages {
			s.unAcked[neighbor][message] = struct{}{}
		}
	}
	s.mu.Unlock()

	// send the response back to the client
	response := map[string]any{
		"type": "topology_ok",
	}
	return s.n.Reply(msg, response)
}

func buildBalanceTree(id string, allNodes []string) []string {
	if len(allNodes) <= 5 {
		if id == "n0" {
			// Node n0 will have all nodes as neighbors
			neighbors := make([]string, 0, len(allNodes)-1)
			for _, node := range allNodes {
				if node != id {
					neighbors = append(neighbors, node)
				}
			}
			return neighbors
		} else {
			// For other nodes, use the topology provided in the message
			return []string{"n0"}
		}

	}

	// balance tree 3 layers
	switch id {
	case "n0":
		return []string{"n1", "n2", "n3", "n4"}
	case "n1":
		return []string{"n0", "n5", "n6", "n7", "n8", "n9"}
	case "n2":
		return []string{"n0", "n10", "n11", "n12", "n13", "n14"}
	case "n3":
		return []string{"n0", "n15", "n16", "n17", "n18", "n19"}
	case "n4":
		return []string{"n0", "n20", "n21", "n22", "n23", "n24"}
	default:
		// For other nodes, connect them to their parent node based on the ID
		var num int
		fmt.Sscanf(id[1:], "%d", &num)
		if num >= 5 && num <= 9 {
			return []string{"n1"}
		} else if num >= 10 && num <= 14 {
			return []string{"n2"}
		} else if num >= 15 && num <= 19 {
			return []string{"n3"}
		} else if num >= 20 && num <= 24 {
			return []string{"n4"}
		}
		// For nodes beyond n24, connect them to n0 as a fallback
		return []string{"n0"}
	}

}
