// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package json

import (
	"bytes"
	"testing"
)

func TestUnmarshalCopiesStrings(t *testing.T) {
	input := []byte(`{"value":"model"}`)
	var got struct {
		Value string `json:"value"`
	}

	if err := Unmarshal(input, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	replacement := []byte("xxxxx")
	valueStart := bytes.Index(input, []byte("model"))
	if valueStart < 0 {
		t.Fatal("test input does not contain the expected value")
	}
	copy(input[valueStart:valueStart+len(replacement)], replacement)

	if got.Value != "model" {
		t.Fatalf("decoded string changed after source mutation: got %q", got.Value)
	}
}
