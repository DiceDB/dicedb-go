package dicedb_test

import (
	"context"
	"fmt"
	"net"

	"github.com/dicedb/dicedb-go"
)

type redisHook struct{}

var _ dicedb.Hook = redisHook{}

func (redisHook) DialHook(hook dicedb.DialHook) dicedb.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		fmt.Printf("dialing %s %s\n", network, addr)
		conn, err := hook(ctx, network, addr)
		fmt.Printf("finished dialing %s %s\n", network, addr)
		return conn, err
	}
}

func (redisHook) ProcessHook(hook dicedb.ProcessHook) dicedb.ProcessHook {
	return func(ctx context.Context, cmd dicedb.Cmder) error {
		fmt.Printf("starting processing: <%s>\n", cmd)
		err := hook(ctx, cmd)
		fmt.Printf("finished processing: <%s>\n", cmd)
		return err
	}
}

func (redisHook) ProcessPipelineHook(hook dicedb.ProcessPipelineHook) dicedb.ProcessPipelineHook {
	return func(ctx context.Context, cmds []dicedb.Cmder) error {
		fmt.Printf("pipeline starting processing: %v\n", cmds)
		err := hook(ctx, cmds)
		fmt.Printf("pipeline finished processing: %v\n", cmds)
		return err
	}
}

func Example_instrumentation() {
	rdb := dicedb.NewClient(&dicedb.Options{
		Addr: ":7379",
	})
	rdb.AddHook(redisHook{})

	rdb.Ping(ctx)
	// Output: starting processing: <ping: >
	// dialing tcp :7379
	// finished dialing tcp :7379
	// finished processing: <ping: PONG>
}

func ExamplePipeline_instrumentation() {
	rdb := dicedb.NewClient(&dicedb.Options{
		Addr: ":7379",
	})
	rdb.AddHook(redisHook{})

	rdb.Pipelined(ctx, func(pipe dicedb.Pipeliner) error {
		pipe.Ping(ctx)
		pipe.Ping(ctx)
		return nil
	})
	// Output: pipeline starting processing: [ping:  ping: ]
	// dialing tcp :7379
	// finished dialing tcp :7379
	// pipeline finished processing: [ping: PONG ping: PONG]
}

//func ExampleClient_Watch_instrumentation() {
//	rdb := dicedb.NewClient(&dicedb.Options{
//		Addr: ":7379",
//	})
//	rdb.AddHook(redisHook{})
//
//	rdb.Watch(ctx, func(tx *dicedb.Tx) error {
//		tx.Ping(ctx)
//		tx.Ping(ctx)
//		return nil
//	}, "foo")
//	// Output:
//	// starting processing: <watch foo: >
//	// dialing tcp :7379
//	// finished dialing tcp :7379
//	// finished processing: <watch foo: OK>
//	// starting processing: <ping: >
//	// finished processing: <ping: PONG>
//	// starting processing: <ping: >
//	// finished processing: <ping: PONG>
//	// starting processing: <unwatch: >
//	// finished processing: <unwatch: OK>
//}
