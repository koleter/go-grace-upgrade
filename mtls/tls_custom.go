/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tls

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"strconv"
	"sync"
	"time"
)

// HandshakeInfoLog and HandshakeDebugLog are optional hooks used to route TLS
// handshake milestone logs into the upper layer's logger (e.g. pkg/mtls wires
// them to MOSN's logger). They are nil by default so that this forked
// crypto/tls package keeps no hard dependency on the logging package, which
// would otherwise introduce an import cycle (tls -> log -> types -> tls).
var (
	HandshakeInfoLog  func(traceID, format string, args ...interface{})
	HandshakeDebugLog func(traceID, format string, args ...interface{})
)

// newHandshakeTraceID generates a short random id used to correlate all logs
// belonging to the same TLS handshake.
func newHandshakeTraceID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b[:])
}

// ensureTraceID assigns a trace id to the connection if it does not have one
// yet and returns it. It is called at the very beginning of the handshake,
// which is already serialized by handshakeMutex, so no extra locking is needed.
func (c *Conn) ensureTraceID() string {
	if c.traceID == "" {
		c.traceID = newHandshakeTraceID()
	}
	return c.traceID
}

// hsInfof logs a handshake milestone at info level with the trace id prefixed.
func (c *Conn) hsInfof(format string, args ...interface{}) {
	if HandshakeInfoLog != nil {
		HandshakeInfoLog(c.traceID, format, args...)
	}
}

// hsDebugf logs a verbose handshake detail at debug level with the trace id
// prefixed.
func (c *Conn) hsDebugf(format string, args ...interface{}) {
	if HandshakeDebugLog != nil {
		HandshakeDebugLog(c.traceID, format, args...)
	}
}

var globalStore = &certificateStore{
	store: map[string]*x509.Certificate{},
}

// LoadOrStoreCertificate is a wrapper for globalStore.LoadOrStoreCertificate
// If the input bytes can be parsed as a x509 certificate, get the certificate
// from a cache, or create it and store in cache.
func LoadOrStoreCertificate(b []byte) (*x509.Certificate, error) {
	return globalStore.LoadOrStoreCertificate(b)
}

type certificateStore struct {
	rwmutex sync.RWMutex
	store   map[string]*x509.Certificate
}

func (s *certificateStore) LoadOrStoreCertificate(b []byte) (*x509.Certificate, error) {
	if cert, ok := s.load(b); ok {
		return cert, nil
	}
	// try to create a new certificate
	s.rwmutex.Lock()
	defer s.rwmutex.Unlock()
	// double check the certificate exists
	x509Cert, ok := s.store[string(b)]
	if !ok {
		var err error
		x509Cert, err = x509.ParseCertificate(b)
		if err != nil {
			return nil, err
		}
		s.store[string(b)] = x509Cert
	}
	return x509Cert, nil
}

func (s *certificateStore) load(b []byte) (*x509.Certificate, bool) {
	s.rwmutex.RLock()
	defer s.rwmutex.RUnlock()
	// Use string([]byte) in map key instead of key := string(b) out of map
	// to reduce copy and gc.
	// see details in: https://github.com/golang/go/issues/3512
	cert, ok := s.store[string(b)]
	return cert, ok
}
