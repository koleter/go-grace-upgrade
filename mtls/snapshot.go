package tls

import (
	"bytes"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
)

// Snapshot is a versioned, application-data TLS connection checkpoint.
// It contains traffic keys. Transfer it only over the private upgrade channel;
// never log it or persist it in public files. Quiesce all I/O before taking it.
type Snapshot struct {
	Format                                           int
	Version, CipherSuite                             uint16
	IsClient                                         bool
	MasterSecret, ClientRandom, ServerRandom         []byte
	InTrafficSecret, OutTrafficSecret                []byte
	InSeq, OutSeq                                    [8]byte
	RawInput, Input, Hand, SendBuf                   []byte
	ServerName, Protocol                             string
	DidResume, ProtocolFallback, ClientFinishedFirst bool
	PeerCertificates                                 [][]byte
	VerifiedChains                                   [][][]byte
	OCSP                                             []byte
	SCT                                              [][]byte
	ClientFinished, ServerFinished                   [12]byte
	ResumptionSecret                                 []byte
	ExporterSecret                                   []byte
	BytesSent                                        int64
	PacketsSent                                      int64
	RetryCount                                       int
	Handshakes                                       int
	SecureRenegotiation                              bool
}

// TLS 1.3 keeps current traffic secrets in halfConn. TLS 1.2 needs the
// handshake secrets retained separately to reconstruct the record ciphers.
type tls12Secrets struct {
	masterSecret, clientRandom, serverRandom []byte
}

func (c *Conn) saveTLS12Secrets(masterSecret, clientRandom, serverRandom []byte) {
	c.tls12 = &tls12Secrets{bytes.Clone(masterSecret), bytes.Clone(clientRandom), bytes.Clone(serverRandom)}
}

// Snapshot exports a non-destructive copy, including partial TLS records,
// decrypted unread bytes, handshake fragments, and current TLS 1.3 traffic keys.
func (c *Conn) Snapshot() (*Snapshot, error) {
	c.handshakeMutex.Lock()
	defer c.handshakeMutex.Unlock()
	c.in.Lock()
	defer c.in.Unlock()
	c.out.Lock()
	defer c.out.Unlock()
	if !c.handshakeComplete() || c.closeNotifySent || c.in.err != nil || c.out.err != nil {
		return nil, errors.New("tls: cannot snapshot an incomplete, closed or failed connection")
	}
	s := &Snapshot{Format: 1, Version: c.vers, CipherSuite: c.cipherSuite, IsClient: c.isClient,
		InSeq: c.in.seq, OutSeq: c.out.seq, RawInput: bytes.Clone(c.rawInput.Bytes()),
		Hand: bytes.Clone(c.hand.Bytes()), SendBuf: bytes.Clone(c.sendBuf),
		ServerName: c.serverName, Protocol: c.clientProtocol, DidResume: c.didResume,
		ProtocolFallback: c.clientProtocolFallback, ClientFinishedFirst: c.clientFinishedIsFirst,
		OCSP: bytes.Clone(c.ocspResponse), SCT: cloneSlices(c.scts),
		ClientFinished: c.clientFinished, ServerFinished: c.serverFinished,
		ResumptionSecret: bytes.Clone(c.resumptionSecret), ExporterSecret: bytes.Clone(c.exporterSecret), BytesSent: c.bytesSent,
		PacketsSent: c.packetsSent, RetryCount: c.retryCount,
		Handshakes: c.handshakes, SecureRenegotiation: c.secureRenegotiation,
	}
	s.Input = make([]byte, c.input.Len())
	c.input.ReadAt(s.Input, c.input.Size()-int64(c.input.Len()))
	for _, cert := range c.peerCertificates {
		s.PeerCertificates = append(s.PeerCertificates, bytes.Clone(cert.Raw))
	}
	for _, chain := range c.verifiedChains {
		var raw [][]byte
		for _, cert := range chain {
			raw = append(raw, bytes.Clone(cert.Raw))
		}
		s.VerifiedChains = append(s.VerifiedChains, raw)
	}
	switch c.vers {
	case VersionTLS12:
		if c.tls12 == nil || len(c.tls12.masterSecret) != 48 {
			return nil, errors.New("tls: missing TLS 1.2 master secret")
		}
		suite := findSuite12(c.cipherSuite)
		if suite == nil || suite.cipher == nil && suite.aead == nil {
			return nil, errors.New("tls: unsupported TLS 1.2 cipher")
		}
		// TLS 1.2 AEAD and CBC use explicit per-record state. Stateful stream
		// ciphers cannot be reconstructed from the original keys alone.
		if suite.aead == nil && suite.ivLen == 0 {
			return nil, errors.New("tls: stream cipher migration unsupported")
		}
		s.MasterSecret, s.ClientRandom, s.ServerRandom = bytes.Clone(c.tls12.masterSecret), bytes.Clone(c.tls12.clientRandom), bytes.Clone(c.tls12.serverRandom)
	case VersionTLS13:
		s.InTrafficSecret, s.OutTrafficSecret = bytes.Clone(c.in.trafficSecret), bytes.Clone(c.out.trafficSecret)
		if len(s.InTrafficSecret) == 0 || len(s.OutTrafficSecret) == 0 {
			return nil, errors.New("tls: missing TLS 1.3 traffic secrets")
		}
	default:
		return nil, fmt.Errorf("tls: migration requires TLS 1.2 or 1.3, got %#x", c.vers)
	}
	return s, nil
}

// Restore reconstructs an established TLS connection without a new handshake.
// The caller must keep the old endpoint paused until ownership commits.
func Restore(raw net.Conn, config *Config, s *Snapshot) (*Conn, error) {
	if s == nil || s.Format != 1 {
		return nil, errors.New("tls: unknown snapshot format")
	}
	if config == nil {
		config = &Config{}
	}
	var c *Conn
	if s.IsClient {
		c = Client(raw, config)
	} else {
		c = Server(raw, config)
	}
	c.vers, c.haveVers, c.cipherSuite = s.Version, true, s.CipherSuite
	switch s.Version {
	case VersionTLS12:
		suite := findSuite12(s.CipherSuite)
		if suite == nil || len(s.MasterSecret) != 48 || len(s.ClientRandom) != 32 || len(s.ServerRandom) != 32 {
			return nil, errors.New("tls: invalid TLS 1.2 snapshot keys")
		}
		if suite.aead == nil && suite.ivLen == 0 {
			return nil, errors.New("tls: stream cipher migration unsupported")
		}
		clientMAC, serverMAC, clientKey, serverKey, clientIV, serverIV := keysFromMasterSecret(
			s.Version, suite, s.MasterSecret, s.ClientRandom, s.ServerRandom, suite.macLen, suite.keyLen, suite.ivLen)
		if s.IsClient {
			clientMAC, serverMAC = serverMAC, clientMAC
			clientKey, serverKey = serverKey, clientKey
			clientIV, serverIV = serverIV, clientIV
		}
		if suite.aead != nil {
			c.in.prepareCipherSpec(s.Version, suite.aead(clientKey, clientIV), nil)
			c.out.prepareCipherSpec(s.Version, suite.aead(serverKey, serverIV), nil)
		} else {
			c.in.prepareCipherSpec(s.Version, suite.cipher(clientKey, clientIV, true), suite.mac(s.Version, clientMAC))
			c.out.prepareCipherSpec(s.Version, suite.cipher(serverKey, serverIV, false), suite.mac(s.Version, serverMAC))
		}
		if err := c.in.changeCipherSpec(); err != nil {
			return nil, err
		}
		if err := c.out.changeCipherSpec(); err != nil {
			return nil, err
		}
		c.saveTLS12Secrets(s.MasterSecret, s.ClientRandom, s.ServerRandom)
		c.ekm = ekmFromMasterSecret(s.Version, suite, s.MasterSecret, s.ClientRandom, s.ServerRandom)
	case VersionTLS13:
		var suite *cipherSuiteTLS13
		for _, candidate := range cipherSuitesTLS13 {
			if candidate.id == s.CipherSuite {
				suite = candidate
				break
			}
		}
		if suite == nil || len(s.InTrafficSecret) != suite.hash.Size() || len(s.OutTrafficSecret) != suite.hash.Size() {
			return nil, errors.New("tls: invalid TLS 1.3 snapshot keys")
		}
		c.in.version, c.out.version = s.Version, s.Version
		c.in.setTrafficSecret(suite, bytes.Clone(s.InTrafficSecret))
		c.out.setTrafficSecret(suite, bytes.Clone(s.OutTrafficSecret))
		if len(s.ExporterSecret) != suite.hash.Size() {
			return nil, errors.New("tls: invalid exporter secret")
		}
		c.exporterSecret = bytes.Clone(s.ExporterSecret)
		c.ekm = suite.exportFromSecret(c.exporterSecret)
	default:
		return nil, errors.New("tls: unsupported snapshot version")
	}
	c.in.seq, c.out.seq = s.InSeq, s.OutSeq
	c.rawInput = *bytes.NewBuffer(bytes.Clone(s.RawInput))
	c.input = *bytes.NewReader(bytes.Clone(s.Input))
	c.hand = *bytes.NewBuffer(bytes.Clone(s.Hand))
	c.sendBuf = bytes.Clone(s.SendBuf)
	c.serverName, c.clientProtocol, c.didResume = s.ServerName, s.Protocol, s.DidResume
	c.clientProtocolFallback, c.clientFinishedIsFirst = s.ProtocolFallback, s.ClientFinishedFirst
	c.ocspResponse, c.scts = bytes.Clone(s.OCSP), cloneSlices(s.SCT)
	c.clientFinished, c.serverFinished = s.ClientFinished, s.ServerFinished
	c.resumptionSecret = bytes.Clone(s.ResumptionSecret)
	c.bytesSent, c.packetsSent, c.retryCount, c.handshakes = s.BytesSent, s.PacketsSent, s.RetryCount, max(1, s.Handshakes)
	c.secureRenegotiation = s.SecureRenegotiation
	for _, der := range s.PeerCertificates {
		cert, err := x509.ParseCertificate(der)
		if err != nil {
			return nil, err
		}
		c.peerCertificates = append(c.peerCertificates, cert)
	}
	for _, ders := range s.VerifiedChains {
		var chain []*x509.Certificate
		for _, der := range ders {
			cert, err := x509.ParseCertificate(der)
			if err != nil {
				return nil, err
			}
			chain = append(chain, cert)
		}
		c.verifiedChains = append(c.verifiedChains, chain)
	}
	atomic.StoreUint32(&c.handshakeStatus, 1)
	return c, nil
}

func findSuite12(id uint16) *cipherSuite {
	for _, suite := range cipherSuites {
		if suite.id == id {
			return suite
		}
	}
	return nil
}

func cloneSlices(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i := range in {
		out[i] = bytes.Clone(in[i])
	}
	return out
}
