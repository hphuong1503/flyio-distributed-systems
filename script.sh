# Create a new folder for the Maelstrom broadcast project
FOLDER_NAME="maelstrom-broadcast"

mkdir -p "$FOLDER_NAME" && cd "$FOLDER_NAME" && \
go mod init "$FOLDER_NAME" && \
go get github.com/jepsen-io/maelstrom/demo/go && \
cat << 'EOF' > main.go
package main

import (
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	// TODO: Sign up your RPC handlers here

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
EOF
echo "✅ Init successful $FOLDER_NAME!"