package wire

type ErrKind int

const (
	NotEstablished ErrKind = 1
	Empty          ErrKind = 2
	Terminated     ErrKind = 3
	CorruptMessage ErrKind = 4
)

type WireError struct {
	Kind  ErrKind
	Cause error
}

func (e WireError) Error() string {
	return e.Cause.Error()
}

func (e WireError) Unwrap() error {
	return e.Cause
}
