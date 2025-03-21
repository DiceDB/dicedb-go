package ironhawk

import (
	"encoding/binary"
	"fmt"
	"github.com/dicedb/dicedb-go/wire"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
)

const (
	maxMessageSize = 32 * 1024 * 1024 // 32 MB
	headerSize     = 4
)

func Read(conn net.Conn) (*wire.Response, error) {
	headerBuffer := make([]byte, headerSize)
	if _, err := io.ReadFull(conn, headerBuffer); err != nil {
		return nil, fmt.Errorf("failed to read message header: %w", err)
	}

	messageSize := binary.BigEndian.Uint32(headerBuffer)
	if messageSize == 0 {
		return nil, fmt.Errorf("invalid message size: 0 bytes")
	}
	if messageSize > uint32(maxMessageSize) {
		return nil, fmt.Errorf("message too large: %d bytes (max: %d)", messageSize, maxMessageSize)
	}

	messageBuffer := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, messageBuffer); err != nil {
		return nil, fmt.Errorf("failed to read message into buffer: %w", err)
	}

	response := &wire.Response{}
	if err := proto.Unmarshal(messageBuffer, response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return response, nil
}

func Write(conn net.Conn, cmd *wire.Command) error {
	message, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	messageSize := len(message)
	messageBuffer := make([]byte, headerSize+messageSize)
	binary.BigEndian.PutUint32(messageBuffer[:headerSize], uint32(messageSize))
	copy(messageBuffer[headerSize:], message)

	if _, err := conn.Write(messageBuffer); err != nil {
		return err
	}

	return nil
}
