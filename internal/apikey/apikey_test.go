package apikey

import (
	"strings"
	"testing"
)

const testSecret = "test-secret-test-secret-test-secret-123"

func signer(t *testing.T, secret string) *Signer {
	t.Helper()
	s, err := NewSigner(secret)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestGenerateValidates(t *testing.T) {
	s := signer(t, testSecret)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		k, err := s.Generate()
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Validate(k); err != nil {
			t.Fatalf("generated key %q failed validation: %v", k, err)
		}
		if seen[k] {
			t.Fatal("duplicate key generated")
		}
		seen[k] = true
	}
}

func TestValidateRejects(t *testing.T) {
	s := signer(t, testSecret)
	k, _ := s.Generate()
	b := []byte(k)
	if b[5] == 'a' {
		b[5] = 'b'
	} else {
		b[5] = 'a'
	}
	cases := map[string]string{
		"empty":         "",
		"wrong prefix":  "xx_" + k[3:],
		"too short":     k[:len(k)-1],
		"too long":      k + "a",
		"bad char":      k[:10] + "-" + k[11:],
		"typo":          string(b),
		"just a prefix": "ll_",
	}
	for name, c := range cases {
		if err := s.Validate(c); err == nil {
			t.Errorf("%s: expected rejection for %q", name, c)
		}
	}
}

// The point of the HMAC: someone without the secret can't produce keys that
// pass the format check, so made-up keys never reach the database.
func TestForgedKeysRejected(t *testing.T) {
	ours := signer(t, testSecret)
	theirs := signer(t, "a-different-secret-a-different-secret!!")
	passed := 0
	for i := 0; i < 2000; i++ {
		forged, _ := theirs.Generate()
		if ours.Validate(forged) == nil {
			passed++
		}
	}
	if passed != 0 {
		t.Fatalf("%d forged keys passed validation", passed)
	}
}

func TestDeriveIsStableAndValid(t *testing.T) {
	s := signer(t, testSecret)
	a, b := s.Derive("demo"), s.Derive("demo")
	if a != b || s.Validate(a) != nil {
		t.Fatalf("derived key unstable or invalid: %q %q", a, b)
	}
	if s.Derive("other") == a {
		t.Fatal("different labels must give different keys")
	}
	if signer(t, "another-secret-another-secret-another!").Derive("demo") == a {
		t.Fatal("different secrets must give different keys")
	}
}

func TestShortSecretRefused(t *testing.T) {
	if _, err := NewSigner("short"); err == nil {
		t.Fatal("short secret accepted")
	}
}

func TestHashAndPrefix(t *testing.T) {
	k, _ := signer(t, testSecret).Generate()
	if len(Hash(k)) != 32 {
		t.Fatal("expected 32-byte sha256")
	}
	p := DisplayPrefix(k)
	if !strings.HasPrefix(k, p) || len(p) != 11 {
		t.Fatalf("bad display prefix %q", p)
	}
}
