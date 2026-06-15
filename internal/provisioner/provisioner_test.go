package provisioner

import (
	"errors"
	"fmt"
	"testing"
)

func TestClassifyNilIsNil(t *testing.T) {
	if got := Classify(nil); got != nil {
		t.Errorf("Classify(nil) = %v, want nil", got)
	}
}

func TestClassifyTransientPatterns(t *testing.T) {
	cases := []string{
		"Cannot connect to the Docker daemon at unix:///var/run/docker.sock",
		"context deadline exceeded waiting for image",
		"dial tcp 127.0.0.1:5432: connect: connection refused",
		"dial tcp: lookup nonexistent on 8.8.8.8:53: no such host",
		"read tcp i/o timeout",
		"docker daemon went away",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			got := Classify(errors.New(msg))
			if !errors.Is(got, ErrTransient) {
				t.Errorf("Classify(%q) not wrapped with ErrTransient", msg)
			}
		})
	}
}

func TestClassifyPermanentPatternsArePassedThrough(t *testing.T) {
	// These are deterministic failures the user must fix; retrying will
	// just produce the same error.
	cases := []string{
		"image infraforge/echo:dev not present locally; build it once",
		"no such image: postgres:99-bogus",
		"invalid bind mount source",
		"port 5432/tcp: already allocated",
	}
	for _, msg := range cases {
		t.Run(msg, func(t *testing.T) {
			in := errors.New(msg)
			got := Classify(in)
			if errors.Is(got, ErrTransient) {
				t.Errorf("Classify(%q) wrongly classified as transient", msg)
			}
			if got.Error() != in.Error() {
				t.Errorf("Classify returned a wrapped error %v; permanent should pass through unchanged", got)
			}
		})
	}
}

func TestClassifyPreservesOriginalMessage(t *testing.T) {
	original := errors.New("Cannot connect to the Docker daemon: socket missing")
	got := Classify(original)
	if !errors.Is(got, ErrTransient) {
		t.Fatalf("expected wrap with ErrTransient")
	}
	// The wrapper string should still mention the underlying cause so
	// log output remains useful.
	wrapped := fmt.Sprintf("%v", got)
	if wrapped == ErrTransient.Error() {
		t.Errorf("wrapped error lost the underlying message: %q", wrapped)
	}
}
