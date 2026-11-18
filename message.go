package graceupgrade

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// Codec describes stateless framing. Decode returns one complete message and
// the number of wire bytes it occupies. Incomplete input returns (0, nil, nil)
// without consuming anything. Encode frames a response payload. Name identifies
// the wire format, including its settings/version, and must match on upgrade.
// The same Codec is shared by connections: its methods must be concurrency-safe,
// must not retain input slices, and must not keep per-connection parser state.
// Name must be stable for the lifetime of the server and contain 1 to 256 bytes.
type Codec interface {
	Name() string
	Decode(input []byte) (consumed int, message []byte, err error)
	Encode(message []byte) ([]byte, error)
}

// LineCodec frames messages with LF. Payloads exclude LF, but retain any CR.
// Response payloads must not contain LF. An empty non-nil response sends a
// blank line; a nil response sends nothing.
func LineCodec() Codec {
	return lineCodec{}
}

type lineCodec struct{}

func (lineCodec) Name() string { return "graceupgrade/line-lf/v1" }

func (lineCodec) Decode(input []byte) (int, []byte, error) {
	i := bytes.IndexByte(input, '\n')
	if i < 0 {
		return 0, nil, nil
	}
	return i + 1, input[:i], nil
}

func (lineCodec) Encode(message []byte) ([]byte, error) {
	if bytes.IndexByte(message, '\n') >= 0 {
		return nil, errors.New("line response contains LF")
	}
	return append(bytes.Clone(message), '\n'), nil
}

// LengthPrefixCodec uses an unsigned big-endian payload length, excluding the
// prefix itself. width must be 1, 2, 4 or 8 bytes. Decode/Encode hide the prefix
// from the business callback. MaxBuffer still bounds the entire framed message.
func LengthPrefixCodec(width int) (Codec, error) {
	if width != 1 && width != 2 && width != 4 && width != 8 {
		return nil, errors.New("graceupgrade: length prefix width must be 1, 2, 4 or 8")
	}
	return lengthPrefixCodec{width: width}, nil
}

type lengthPrefixCodec struct{ width int }

func (c lengthPrefixCodec) Name() string {
	return fmt.Sprintf("graceupgrade/length-be-%d/v1", c.width)
}

func (c lengthPrefixCodec) Decode(input []byte) (int, []byte, error) {
	if len(input) < c.width {
		return 0, nil, nil
	}
	var header [8]byte
	copy(header[8-c.width:], input[:c.width])
	size := binary.BigEndian.Uint64(header[:])
	if size > uint64(maxBufferSize-c.width) {
		return 0, nil, errors.New("length-prefixed frame exceeds 256 KiB")
	}
	total := c.width + int(size)
	if len(input) < total {
		return 0, nil, nil
	}
	return total, input[c.width:total], nil
}

func (c lengthPrefixCodec) Encode(message []byte) ([]byte, error) {
	size := uint64(len(message))
	if c.width < 8 && size >= uint64(1)<<(8*c.width) {
		return nil, errors.New("response length does not fit prefix")
	}
	if len(message) > maxBufferSize-c.width {
		return nil, errors.New("length-prefixed response exceeds 256 KiB")
	}
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], size)
	out := make([]byte, c.width+len(message))
	copy(out, header[8-c.width:])
	copy(out[c.width:], message)
	return out, nil
}

// Session is framework-managed connection state. Sequence starts at 1 and is
// incremented before each message callback. Values can hold authentication or
// other small session data. Both survive upgrades automatically, with a combined
// serialized limit of 1 MiB. Only access Session during OnMessage; do not retain
// it or its map for use in background goroutines. Sequence must not be modified.
type Session struct {
	Sequence uint64            `json:"sequence"`
	Values   map[string][]byte `json:"values,omitempty"`
}

// MessageHandler processes one complete payload. Callbacks for one connection
// are serialized; different connections run concurrently. The framework owns
// framing, all socket I/O and output buffers. Input is valid only during the
// callback. Return nil output for no response, or a non-nil empty slice for an
// empty framed response. Do not modify returned output after returning.
// Finish work before returning; pending goroutines and
// external side effects are not snapshotted or replayed during upgrades.
type MessageHandler func(ctx context.Context, session *Session, message []byte) ([]byte, error)

type messageHandler struct {
	codec     Codec
	onMessage MessageHandler
	state     Session
}

type messageState struct {
	Format  string  `json:"format"`
	Codec   string  `json:"codec"`
	Session Session `json:"session"`
}

const messageStateFormat = "graceupgrade/messages/v1"

func (o *ServerOptions) validateMessages() error {
	if o.OnMessage == nil {
		return errors.New("graceupgrade: OnMessage is required")
	}
	if o.Codec == nil {
		return errors.New("graceupgrade: Codec is required")
	}
	if name := o.Codec.Name(); name == "" || len(name) > 256 {
		return errors.New("graceupgrade: Codec.Name must contain 1 to 256 bytes")
	}
	return nil
}

func (s *Server) newMessageHandler(data []byte) (*messageHandler, error) {
	h := &messageHandler{codec: s.options.Codec, onMessage: s.options.OnMessage}
	if data != nil {
		var state messageState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		if state.Format != messageStateFormat || state.Codec != h.codec.Name() {
			return nil, errors.New("message state format or Codec changed across upgrade")
		}
		h.state = state.Session
	}
	if h.state.Values == nil {
		h.state.Values = make(map[string][]byte)
	}
	return h, nil
}

func (h *messageHandler) Handle(ctx context.Context, input []byte) (int, []byte, error) {
	n, message, err := h.codec.Decode(input)
	if err != nil {
		return 0, nil, err
	}
	if n == 0 {
		if message != nil {
			return 0, nil, errors.New("Codec returned a message without consuming input")
		}
		return 0, nil, nil
	}
	if n < 0 || n > len(input) {
		return 0, nil, errors.New("Codec returned invalid consumption")
	}
	if h.state.Sequence == ^uint64(0) {
		return 0, nil, errors.New("message sequence overflow")
	}
	h.state.Sequence++
	out, err := h.onMessage(ctx, &h.state, message)
	if err != nil || out == nil {
		return n, nil, err
	}
	out, err = h.codec.Encode(out)
	return n, out, err
}

func (h *messageHandler) Snapshot() ([]byte, error) {
	return json.Marshal(messageState{Format: messageStateFormat, Codec: h.codec.Name(), Session: h.state})
}
