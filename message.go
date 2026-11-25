package graceupgrade

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

// Decoder splits inbound bytes into complete messages. Decode returns one
// message and the number of wire bytes it occupies. Incomplete input returns
// (0, nil, nil) without consuming anything. Name identifies the wire format,
// including its settings/version, and must match on upgrade.
// A Decoder is shared by connections: its methods must be concurrency-safe,
// must not retain input slices, and must not keep per-connection parser state.
// Name must be stable for the lifetime of the server and contain 1 to 256 bytes.
type Decoder interface {
	Name() string
	Decode(input []byte) (consumed int, message []byte, err error)
}

// Encoder transforms an outbound response payload into wire bytes. It is
// independent of the inbound Decoder. Name follows the same rules as
// Decoder.Name and identifies the outbound format for upgrade compatibility.
// An Encoder is shared by connections and must be concurrency-safe and stateless.
// It must not retain input slices. Returned bytes must not be modified later.
type Encoder interface {
	Name() string
	Encode(message []byte) ([]byte, error)
}

// DelimiterDecoder splits inbound messages at an exact, non-empty delimiter
// string and removes it. Partial delimiters remain in the framework buffer.
// The delimiter must fit within the framework's 256 KiB buffer limit.
func DelimiterDecoder(delimiter string) (Decoder, error) {
	framing, err := newDelimiterFraming(delimiter)
	if err != nil {
		return nil, err
	}
	return delimiterDecoder{framing}, nil
}

// DelimiterEncoder appends an exact, non-empty delimiter to outbound messages.
// It does not escape payloads: an earlier delimiter in the encoded frame is an
// error. Use RawEncoder when the business callback supplies complete wire bytes.
// An empty non-nil response sends only the delimiter. Each complete frame must
// fit within the framework's 256 KiB buffer limit.
func DelimiterEncoder(delimiter string) (Encoder, error) {
	framing, err := newDelimiterFraming(delimiter)
	if err != nil {
		return nil, err
	}
	return delimiterEncoder{framing}, nil
}

// RawEncoder sends response bytes unchanged. It is the default when
// ServerOptions.Decoder is set and ServerOptions.Encoder is nil.
func RawEncoder() Encoder { return rawEncoder{} }

type rawEncoder struct{}

func (rawEncoder) Name() string { return "graceupgrade/raw/v1" }

func (rawEncoder) Encode(message []byte) ([]byte, error) {
	if len(message) > maxBufferSize {
		return nil, errors.New("raw response exceeds 256 KiB")
	}
	return bytes.Clone(message), nil
}

func newDelimiterFraming(delimiter string) (delimiterFraming, error) {
	if delimiter == "" || len(delimiter) > maxBufferSize {
		return delimiterFraming{}, errors.New("graceupgrade: delimiter length must be between 1 byte and 256 KiB")
	}
	name := fmt.Sprintf("graceupgrade/delimiter-%x/v1", sha256.Sum256([]byte(delimiter)))
	if delimiter == "\n" {
		// Preserve the protocol ID used by existing LF-framed snapshots.
		name = "graceupgrade/line-lf/v1"
	}
	return delimiterFraming{delimiter: []byte(delimiter), name: name}, nil
}

type delimiterFraming struct {
	delimiter []byte
	name      string
}

func (f delimiterFraming) Name() string { return f.name }

type delimiterDecoder struct{ delimiterFraming }

func (d delimiterDecoder) Decode(input []byte) (int, []byte, error) {
	i := bytes.Index(input, d.delimiter)
	if i < 0 {
		return 0, nil, nil
	}
	return i + len(d.delimiter), input[:i], nil
}

type delimiterEncoder struct{ delimiterFraming }

func (e delimiterEncoder) Encode(message []byte) ([]byte, error) {
	if len(message) > maxBufferSize-len(e.delimiter) {
		return nil, errors.New("delimiter-framed response exceeds 256 KiB")
	}
	out := make([]byte, len(message)+len(e.delimiter))
	copy(out, message)
	copy(out[len(message):], e.delimiter)
	if bytes.Index(out, e.delimiter) != len(message) {
		return nil, errors.New("response contains delimiter or overlaps with appended delimiter")
	}
	return out, nil
}

// LengthPrefixDecoder splits inbound messages using an unsigned big-endian
// payload length, excluding the prefix itself. width must be 1, 2, 4 or 8 bytes.
// Decode removes the prefix. MaxBuffer still bounds the entire framed message.
func LengthPrefixDecoder(width int) (Decoder, error) {
	framing, err := newLengthPrefixFraming(width)
	if err != nil {
		return nil, err
	}
	return lengthPrefixDecoder{framing}, nil
}

// LengthPrefixEncoder prepends an unsigned big-endian payload length to
// outbound messages. width must be 1, 2, 4 or 8 bytes. The length excludes the
// prefix itself. MaxBuffer still bounds the entire framed message.
func LengthPrefixEncoder(width int) (Encoder, error) {
	framing, err := newLengthPrefixFraming(width)
	if err != nil {
		return nil, err
	}
	return lengthPrefixEncoder{framing}, nil
}

func newLengthPrefixFraming(width int) (lengthPrefixFraming, error) {
	if width != 1 && width != 2 && width != 4 && width != 8 {
		return lengthPrefixFraming{}, errors.New("graceupgrade: length prefix width must be 1, 2, 4 or 8")
	}
	return lengthPrefixFraming{width: width}, nil
}

type lengthPrefixFraming struct{ width int }

func (f lengthPrefixFraming) Name() string {
	return fmt.Sprintf("graceupgrade/length-be-%d/v1", f.width)
}

type lengthPrefixDecoder struct{ lengthPrefixFraming }

func (d lengthPrefixDecoder) Decode(input []byte) (int, []byte, error) {
	if len(input) < d.width {
		return 0, nil, nil
	}
	var header [8]byte
	copy(header[8-d.width:], input[:d.width])
	size := binary.BigEndian.Uint64(header[:])
	if size > uint64(maxBufferSize-d.width) {
		return 0, nil, errors.New("length-prefixed frame exceeds 256 KiB")
	}
	total := d.width + int(size)
	if len(input) < total {
		return 0, nil, nil
	}
	return total, input[d.width:total], nil
}

type lengthPrefixEncoder struct{ lengthPrefixFraming }

func (e lengthPrefixEncoder) Encode(message []byte) ([]byte, error) {
	size := uint64(len(message))
	if e.width < 8 && size >= uint64(1)<<(8*e.width) {
		return nil, errors.New("response length does not fit prefix")
	}
	if len(message) > maxBufferSize-e.width {
		return nil, errors.New("length-prefixed response exceeds 256 KiB")
	}
	var header [8]byte
	binary.BigEndian.PutUint64(header[:], size)
	out := make([]byte, e.width+len(message))
	copy(out, header[8-e.width:])
	copy(out[e.width:], message)
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
// callback. Return nil output for no response, or a non-nil empty slice to
// pass an empty response through Encoder (RawEncoder emits no bytes).
// Do not modify returned output after returning.
// Finish work before returning; pending goroutines and
// external side effects are not snapshotted or replayed during upgrades.
type MessageHandler func(ctx context.Context, session *Session, message []byte) ([]byte, error)

type messageHandler struct {
	decoder   Decoder
	encoder   Encoder
	protocol  string
	onMessage MessageHandler
	state     Session
}

type messageState struct {
	Format   string  `json:"format"`
	Protocol string  `json:"codec"` // Keep the snapshot key stable across API changes.
	Session  Session `json:"session"`
}

const messageStateFormat = "graceupgrade/messages/v1"

func (o ServerOptions) messageFraming() (Decoder, Encoder) {
	encoder := o.Encoder
	if encoder == nil {
		encoder = RawEncoder()
	}
	return o.Decoder, encoder
}

func messageProtocolName(decoder Decoder, encoder Encoder) string {
	inbound, outbound := decoder.Name(), encoder.Name()
	if inbound == outbound {
		// Preserve protocol IDs used by existing symmetric framing snapshots.
		return inbound
	}
	// Quoting keeps the two names unambiguous even if they contain separators.
	return fmt.Sprintf("graceupgrade/pipeline-%x/v1", sha256.Sum256([]byte(fmt.Sprintf("%q/%q", inbound, outbound))))
}

func (o *ServerOptions) validateMessages() error {
	if o.OnMessage == nil {
		return errors.New("graceupgrade: OnMessage is required")
	}
	decoder, encoder := o.messageFraming()
	if decoder == nil {
		return errors.New("graceupgrade: Decoder is required")
	}
	if name := decoder.Name(); name == "" || len(name) > 256 {
		return errors.New("graceupgrade: Decoder.Name must contain 1 to 256 bytes")
	}
	if name := encoder.Name(); name == "" || len(name) > 256 {
		return errors.New("graceupgrade: Encoder.Name must contain 1 to 256 bytes")
	}
	return nil
}

func (s *Server) newMessageHandler(data []byte) (*messageHandler, error) {
	decoder, encoder := s.options.messageFraming()
	h := &messageHandler{
		decoder: decoder, encoder: encoder, protocol: messageProtocolName(decoder, encoder),
		onMessage: s.options.OnMessage,
	}
	if data != nil {
		var state messageState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		if state.Format != messageStateFormat || state.Protocol != h.protocol {
			return nil, errors.New("message state format, Decoder or Encoder changed across upgrade")
		}
		h.state = state.Session
	}
	if h.state.Values == nil {
		h.state.Values = make(map[string][]byte)
	}
	return h, nil
}

func (h *messageHandler) Handle(ctx context.Context, input []byte) (int, []byte, error) {
	n, message, err := h.decoder.Decode(input)
	if err != nil {
		return 0, nil, err
	}
	if n == 0 {
		if message != nil {
			return 0, nil, errors.New("Decoder returned a message without consuming input")
		}
		return 0, nil, nil
	}
	if n < 0 || n > len(input) {
		return 0, nil, errors.New("Decoder returned invalid consumption")
	}
	if h.state.Sequence == ^uint64(0) {
		return 0, nil, errors.New("message sequence overflow")
	}
	h.state.Sequence++
	out, err := h.onMessage(ctx, &h.state, message)
	if err != nil || out == nil {
		return n, nil, err
	}
	out, err = h.encoder.Encode(out)
	return n, out, err
}

func (h *messageHandler) Snapshot() ([]byte, error) {
	return json.Marshal(messageState{Format: messageStateFormat, Protocol: h.protocol, Session: h.state})
}
