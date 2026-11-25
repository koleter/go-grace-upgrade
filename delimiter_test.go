package graceupgrade

import (
	"bytes"
	"strings"
	"testing"
)

func testDelimiterDecoder(t *testing.T, delimiter string) Decoder {
	t.Helper()
	decoder, err := DelimiterDecoder(delimiter)
	if err != nil {
		t.Fatal(err)
	}
	return decoder
}

func testDelimiterEncoder(t *testing.T, delimiter string) Encoder {
	t.Helper()
	encoder, err := DelimiterEncoder(delimiter)
	if err != nil {
		t.Fatal(err)
	}
	return encoder
}

func TestDelimiterFramingHandlesFragmentsAndEmptyMessages(t *testing.T) {
	for _, delimiter := range []string{"\n", "\r\n", "||", "<END>", "结束", "\x00\xff", "aba"} {
		t.Run(delimiter, func(t *testing.T) {
			decoder := testDelimiterDecoder(t, delimiter)
			encoder := testDelimiterEncoder(t, delimiter)
			payloads := [][]byte{[]byte("hello"), {}, []byte("world")}
			var input []byte
			for _, payload := range payloads {
				frame, err := encoder.Encode(payload)
				if err != nil {
					t.Fatal(err)
				}
				want := append(bytes.Clone(payload), delimiter...)
				if !bytes.Equal(frame, want) {
					t.Fatalf("encoded frame = %q, want %q", frame, want)
				}
				// Include splits within a multi-byte delimiter, including UTF-8.
				for cut := 0; cut < len(frame); cut++ {
					if n, data, err := decoder.Decode(frame[:cut]); err != nil || n != 0 || data != nil {
						t.Fatalf("partial frame consumed at %d: %d %q %v", cut, n, data, err)
					}
				}
				input = append(input, frame...)
			}
			input = append(input, "unfinished"...)
			for _, payload := range payloads {
				n, data, err := decoder.Decode(input)
				if err != nil || n != len(payload)+len(delimiter) || data == nil || !bytes.Equal(data, payload) {
					t.Fatalf("frame decode: %d %q %v, want %q", n, data, err, payload)
				}
				input = input[n:]
			}
			if n, data, err := decoder.Decode(input); err != nil || n != 0 || data != nil || string(input) != "unfinished" {
				t.Fatalf("incomplete trailing frame consumed: %d %q %v", n, data, err)
			}
		})
	}
}

func TestDelimiterEncoderRejectsAmbiguousResponses(t *testing.T) {
	for _, test := range []struct{ delimiter, payload string }{
		{"\n", "two\nlines"},
		{"\r\n", "two\r\nlines"},
		{"<END>", "one<END>two"},
		{"||", "ends in |"},
		{"aba", "ab"},
	} {
		encoder := testDelimiterEncoder(t, test.delimiter)
		if frame, err := encoder.Encode([]byte(test.payload)); err == nil || frame != nil {
			t.Fatalf("ambiguous response %q with delimiter %q: %q %v", test.payload, test.delimiter, frame, err)
		}
	}
	// A fragment of the delimiter is valid when it creates no earlier match.
	encoder := testDelimiterEncoder(t, "\r\n")
	payload := []byte("line\nwith\rfragments\r")
	frame, err := encoder.Encode(payload)
	if err != nil {
		t.Fatal(err)
	}
	if n, data, err := testDelimiterDecoder(t, "\r\n").Decode(frame); err != nil || n != len(frame) || !bytes.Equal(data, payload) {
		t.Fatalf("delimiter fragment altered payload: %d %q %v", n, data, err)
	}
}

func TestDelimiterEncoderOwnsEncodedBuffer(t *testing.T) {
	encoder := testDelimiterEncoder(t, "<END>")
	backing := []byte("hellounchanged")
	message := backing[:5]
	frame, err := encoder.Encode(message)
	if err != nil {
		t.Fatal(err)
	}
	if string(backing) != "hellounchanged" {
		t.Fatalf("Encode overwrote input backing array: %q", backing)
	}
	message[0] = 'H'
	if string(frame) != "hello<END>" {
		t.Fatalf("encoded frame aliases input: %q", frame)
	}
}

func TestDelimiterFramingValidationAndLimits(t *testing.T) {
	for _, delimiter := range []string{"", strings.Repeat("x", maxBufferSize+1)} {
		if decoder, err := DelimiterDecoder(delimiter); err == nil || decoder != nil {
			t.Fatalf("invalid decoder delimiter accepted: length=%d, err=%v", len(delimiter), err)
		}
		if encoder, err := DelimiterEncoder(delimiter); err == nil || encoder != nil {
			t.Fatalf("invalid encoder delimiter accepted: length=%d, err=%v", len(delimiter), err)
		}
	}
	for _, delimiter := range []string{"\r\n", strings.Repeat("x", maxBufferSize)} {
		decoder := testDelimiterDecoder(t, delimiter)
		encoder := testDelimiterEncoder(t, delimiter)
		message := bytes.Repeat([]byte{'a'}, maxBufferSize-len(delimiter))
		frame, err := encoder.Encode(message)
		if err != nil || len(frame) != maxBufferSize {
			t.Fatalf("maximum frame rejected: length=%d, err=%v", len(frame), err)
		}
		if n, data, err := decoder.Decode(frame); err != nil || n != len(frame) || !bytes.Equal(data, message) {
			t.Fatalf("maximum frame decode: %d %v", n, err)
		}
		if frame, err := encoder.Encode(append(message, 'a')); err == nil || frame != nil {
			t.Fatal("oversized frame accepted")
		}
	}
}

func TestDelimiterFramingNameIncludesConfiguration(t *testing.T) {
	names := make(map[string]string)
	for _, delimiter := range []string{"\n", "\r\n", "|", "||", "结束", strings.Repeat("x", maxBufferSize)} {
		name := testDelimiterDecoder(t, delimiter).Name()
		if len(name) == 0 || len(name) > 256 || name != testDelimiterDecoder(t, delimiter).Name() || name != testDelimiterEncoder(t, delimiter).Name() {
			t.Fatalf("unstable or invalid name for delimiter length %d: %q", len(delimiter), name)
		}
		if previous, ok := names[name]; ok {
			t.Fatalf("delimiters %q and %q have the same name", previous, delimiter)
		}
		names[name] = delimiter
	}
	if name := testDelimiterDecoder(t, "\n").Name(); name != "graceupgrade/line-lf/v1" {
		t.Fatalf("LF framing lost compatibility with existing snapshots: %q", name)
	}
}
