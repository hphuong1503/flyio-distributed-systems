package main

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var counter uint64

	n.Handle("generate", func(msg maelstrom.Message) error {

		seq := atomic.AddUint64(&counter, 1)

		uniqueID := fmt.Sprintf("%s-%d", n.ID(), seq)

		body := map[string]any{
			"type": "generate_ok",
			"id":   uniqueID,
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}

}
