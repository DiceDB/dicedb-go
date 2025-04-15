package internal

import (
	"bytes"
	"io"
	"testing"

	"github.com/dicedb/dicedb-go/mock"
	"go.uber.org/mock/gomock"
)

func TestClosesConnOnClose(t *testing.T) {
	// arrange
	ctrl := gomock.NewController(t)
	mockConn := mock.NewMockConn(ctrl)
	mockConn.EXPECT().Close().Times(1)

	// act
	wire := NewTCPWire(50, mockConn)
	wire.Close()

	// assert
	ctrl.Finish()
}

func TestSend(t *testing.T) {
	// arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	msg := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	prefix := []byte{0, 0, 0, byte(len(msg))}
	want := append(prefix, msg...)

	mockConn := mock.NewMockConn(ctrl)
	var captured []byte
	mockConn.EXPECT().Write(gomock.Any()).MinTimes(1).DoAndReturn(func(buffer []byte) (int, error) {
		captured = append(captured, buffer...)

		return len(buffer), nil
	})

	wire := NewTCPWire(50, mockConn)

	// act
	err := wire.Send(msg)

	// assert
	if err != nil {
		t.Errorf("Send() error = %v", err)
	}

	if !bytes.Equal(captured, want) {
		t.Errorf("Send() captured = %v, want %v", captured, want)
	}
}

func TestReceive(t *testing.T) {
	// arrange
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	want := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	prefix := []byte{0, 0, 0, byte(len(want))}
	msg := append(prefix, want...)
	mockConn := mock.NewMockConn(ctrl)

	pos := 0
	mockConn.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(func(buffer []byte) (int, error) {
		remaining := len(msg) - pos
		if remaining == 0 {
			return 0, io.EOF // End of message
		}

		capacity := len(buffer)
		if capacity > remaining {
			capacity = remaining
		}

		// Copy the appropriate amount of data from msg to the buffer
		copy(buffer, msg[pos:pos+capacity])
		pos += capacity

		// Return the number of bytes read and no error
		return capacity, nil
	})
	wire := NewTCPWire(50, mockConn)

	// act
	buffer, err := wire.Receive()

	// assert
	if err != nil {
		t.Errorf("Receive() error = %v", err)
	}

	if !bytes.Equal(buffer, want) {
		t.Errorf("Receive() captured = %v, want %v", buffer, want)
	}
}
