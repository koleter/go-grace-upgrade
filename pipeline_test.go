package graceupgrade

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

type testEncoder struct {
	encode func([]byte) ([]byte, error)
}

func (testEncoder) Name() string { return "test/encoder/v1" }
func (e testEncoder) Encode(message []byte) ([]byte, error) {
	return e.encode(message)
}

func TestMessagePipelineSeparatesInboundAndOutbound(t *testing.T) {
	encodeErr := errors.New("encode failed")
	failing := testEncoder{encode: func([]byte) ([]byte, error) { return nil, encodeErr }}
	for _, test := range []struct {
		name           string
		encoder        Encoder
		response, want []byte
		wantErr        error
	}{
		{"default-raw", nil, []byte("reply#1\nbody"), []byte("reply#1\nbody"), nil},
		{"explicit-raw", RawEncoder(), []byte("reply#1"), []byte("reply#1"), nil},
		{"newline", testDelimiterEncoder(t, "\n"), []byte("reply#1"), []byte("reply#1\n"), nil},
		{"custom", testEncoder{encode: func(b []byte) ([]byte, error) { return bytes.ToUpper(b), nil }}, []byte("reply#1"), []byte("REPLY#1"), nil},
		{"no-response", failing, nil, nil, nil},
		{"empty-raw", nil, []byte{}, []byte{}, nil},
		{"empty-newline", testDelimiterEncoder(t, "\n"), []byte{}, []byte("\n"), nil},
		{"encode-error", failing, []byte("reply"), nil, encodeErr},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls int
			options := ServerOptions{
				Decoder: testDelimiterDecoder(t, "#"), Encoder: test.encoder,
				OnMessage: func(_ context.Context, session *Session, message []byte) ([]byte, error) {
					calls++
					if string(message) != "hello" || session.Sequence != 1 {
						t.Fatalf("callback input: %q, sequence=%d", message, session.Sequence)
					}
					return test.response, nil
				},
			}
			if err := options.validateMessages(); err != nil {
				t.Fatal(err)
			}
			h, err := (&Server{options: options}).newMessageHandler(nil)
			if err != nil {
				t.Fatal(err)
			}
			if n, out, err := h.Handle(context.Background(), []byte("hello")); n != 0 || out != nil || err != nil || calls != 0 {
				t.Fatalf("partial input reached callback: %d %q %v, calls=%d", n, out, err, calls)
			}
			n, out, err := h.Handle(context.Background(), []byte("hello#unconsumed"))
			if n != 6 || !bytes.Equal(out, test.want) || (out == nil) != (test.want == nil) || !errors.Is(err, test.wantErr) || calls != 1 {
				t.Fatalf("pipeline result: %d %q %v, calls=%d; want %q %v", n, out, err, calls, test.want, test.wantErr)
			}
		})
	}
}

func TestRawEncoderOwnsWireBytesAndBoundsOutput(t *testing.T) {
	encoder := RawEncoder()
	payload := []byte("reply#\n\x00")
	wire, err := encoder.Encode(payload)
	if err != nil || !bytes.Equal(wire, payload) {
		t.Fatalf("raw payload altered: %q %v", wire, err)
	}
	payload[0] = 'R'
	if string(wire) != "reply#\n\x00" {
		t.Fatal("raw wire bytes alias the input")
	}
	if wire, err := encoder.Encode(make([]byte, maxBufferSize)); err != nil || len(wire) != maxBufferSize {
		t.Fatalf("maximum raw response rejected: %d %v", len(wire), err)
	}
	if wire, err := encoder.Encode(make([]byte, maxBufferSize+1)); err == nil || wire != nil {
		t.Fatal("oversized raw response accepted")
	}
}

func TestMessagePipelineRestoresDefaultAndExplicitRawInterchangeably(t *testing.T) {
	options := ServerOptions{
		Decoder: testDelimiterDecoder(t, "#"),
		OnMessage: func(_ context.Context, session *Session, input []byte) ([]byte, error) {
			session.Values["last"] = bytes.Clone(input)
			return input, nil
		},
	}
	server := &Server{options: options}
	h, err := server.newMessageHandler(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.Handle(context.Background(), []byte("first#")); err != nil {
		t.Fatal(err)
	}
	state, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	server.options.Encoder = RawEncoder()
	restored, err := server.newMessageHandler(state)
	if err != nil {
		t.Fatal(err)
	}
	if restored.state.Sequence != 1 || string(restored.state.Values["last"]) != "first" {
		t.Fatalf("session state lost: %+v", restored.state)
	}
	state, err = restored.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	server.options.Encoder = nil
	if _, err := server.newMessageHandler(state); err != nil {
		t.Fatal(err)
	}
}
