package dicedb

import (
	"errors"
	"sync"
	"testing"

	"github.com/dicedb/dicedb-go/wire"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		port    int
		wantNil bool
		err     error
	}{
		{
			name:    "valid connection",
			host:    "localhost",
			port:    7379,
			wantNil: false,
			err:     nil,
		},
		{
			name:    "invalid port",
			host:    "localhost",
			port:    -1,
			wantNil: true,
			err:     errors.New("could not connect to dicedb server after 3 retries: dial tcp: address -1: invalid port"),
		},
		{
			name:    "unable to connect",
			host:    "localhost",
			port:    9999,
			wantNil: true,
			err:     errors.New("could not connect to dicedb server after 3 retries: dial tcp 127.0.0.1:9999: connect: connection refused"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.host, tt.port, &sync.WaitGroup{})
			if (client == nil) != tt.wantNil {
				t.Errorf("NewClient() got = %v, %s, want nil = %v, err = %v", client, err, tt.wantNil, tt.err)
			}
			if err != nil && err.Error() != tt.err.Error() {
				t.Errorf("NewClient() got = %v, %s, want nil = %v, err = %v", client, err, tt.wantNil, tt.err)
			}
		})
	}
}

func TestClient_Fire(t *testing.T) {
	client, err := NewClient("localhost", 7379, &sync.WaitGroup{})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	tests := []struct {
		name     string
		mockConn *Client
		cmd      *wire.Command
		result   *wire.Result
		err      error
	}{
		{
			name:     "successful command",
			mockConn: client,
			cmd:      &wire.Command{Cmd: "PING"},
			result:   &wire.Result{Status: wire.Status_OK, Response: &wire.Result_PINGRes{PINGRes: &wire.PINGRes{Message: "PONG"}}},
			err:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := tt.mockConn.Fire(tt.cmd)
			if tt.err != nil && resp.Status != wire.Status_ERR {
				t.Errorf("Fire() expected error: %v, want: %v", resp.Status, tt.err)
			}
			if resp.GetPINGRes().Message != tt.result.GetPINGRes().Message {
				t.Errorf("Fire() unexpected response: %v, want: %v", resp.GetPINGRes().Message, tt.result.GetPINGRes().Message)
			}
		})
	}
}
