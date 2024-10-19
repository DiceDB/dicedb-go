# Command examples for redis.io

These examples appear on the [DiceDB documentation](https://dicedb.io) site as part of the tabbed examples interface.

## How to add examples

- Create a Go test file with a meaningful name in the current folder. 
- Create a single method prefixed with `Example` and write your test in it.
- Determine the id for the example you're creating and add it as the first line of the file: `// EXAMPLE: set_and_get`.
- We're using the [Testable Examples](https://go.dev/blog/examples) feature of Go to test the desired output has been written to stdout.

## How to test the examples

- Start a DiceDB server locally on port 7379
- CD into the `doctests` directory
- Run `go test` to test all examples in the directory.
- Run `go test filename.go` to test a single file

