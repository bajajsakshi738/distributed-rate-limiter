// Package apikey generates and validates API keys.
//
// A key looks like: ll_<30 random base62 chars><6 char signature>
//
// The signature is a truncated HMAC-SHA256 of the random part, keyed with a
// server secret (KEY_SECRET). Only the server can produce a valid one, so a
// flood of made-up keys is rejected in microseconds by Validate, without a
// cache or database lookup. (A plain checksum like CRC32 would only catch
// typos: anyone can compute it.)
//
// Rotating KEY_SECRET invalidates every existing key, so it is set once.
package apikey

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/big"
	"strings"
)

const (
	Prefix       = "ll_"
	randomLen    = 30
	sigLen       = 6 // 62^6 ≈ 5.7e10 possible signatures (~35 bits)
	Length       = len(Prefix) + randomLen + sigLen
	displayChars = 8 // characters of the random part shown in the UI and logs
	alphabet     = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	MinSecretLen = 32
)

var (
	ErrMalformed   = errors.New("malformed api key")
	ErrShortSecret = errors.New("KEY_SECRET must be at least 32 characters")
)

// Signer issues and checks keys with a server secret.
type Signer struct {
	secret []byte
}

func NewSigner(secret string) (*Signer, error) {
	if len(secret) < MinSecretLen {
		return nil, ErrShortSecret
	}
	return &Signer{secret: []byte(secret)}, nil
}

// Generate returns a new random key. 30 base62 characters is ~178 bits of
// entropy, which is why a fast hash (SHA-256) is fine for storage: there is
// nothing to brute-force, unlike a human-chosen password.
func (s *Signer) Generate() (string, error) {
	max := big.NewInt(int64(len(alphabet)))
	random := make([]byte, randomLen)
	for i := range random {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		random[i] = alphabet[n.Int64()]
	}
	return Prefix + string(random) + s.sign(string(random)), nil
}

// Derive returns a deterministic key for a label (used for the public demo
// key, so every instance and restart agrees on it without storing it).
func (s *Signer) Derive(label string) string {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte("derive:" + label))
	seed := m.Sum(nil)
	random := make([]byte, randomLen)
	for i := range random {
		random[i] = alphabet[int(seed[i%len(seed)]^byte(i*31))%len(alphabet)]
	}
	return Prefix + string(random) + s.sign(string(random))
}

// Validate checks the format and signature only. It does not say whether
// the key exists or is revoked.
func (s *Signer) Validate(key string) error {
	if len(key) != Length || !strings.HasPrefix(key, Prefix) {
		return ErrMalformed
	}
	body := key[len(Prefix):]
	for i := 0; i < len(body); i++ {
		if strings.IndexByte(alphabet, body[i]) < 0 {
			return ErrMalformed
		}
	}
	random, sig := body[:randomLen], body[randomLen:]
	if !hmac.Equal([]byte(s.sign(random)), []byte(sig)) {
		return ErrMalformed
	}
	return nil
}

func (s *Signer) sign(random string) string {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte(random))
	n := binary.BigEndian.Uint64(m.Sum(nil)[:8])
	out := make([]byte, sigLen)
	for i := sigLen - 1; i >= 0; i-- {
		out[i] = alphabet[n%uint64(len(alphabet))]
		n /= uint64(len(alphabet))
	}
	return string(out)
}

// Hash is what we store in Postgres. The raw key is never stored.
func Hash(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// DisplayPrefix is the safe-to-log identifier, e.g. "ll_8fK2pQ1x".
func DisplayPrefix(key string) string {
	if len(key) < len(Prefix)+displayChars {
		return Prefix + "?"
	}
	return key[:len(Prefix)+displayChars]
}
