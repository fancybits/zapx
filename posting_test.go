//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zap

import (
	"encoding/binary"
	"strings"
	"testing"
)

// uvarints encodes a location record the way the location chunk stores it:
// field, pos, start, end, numArrayPos.
func uvarints(vals ...uint64) []byte {
	var buf []byte
	var b [binary.MaxVarintLen64]byte
	for _, v := range vals {
		buf = append(buf, b[:binary.PutUvarint(b[:], v)]...)
	}
	return buf
}

// A damaged location chunk decodes a field id that the segment never wrote.
// Indexing fieldsInv with it panics and takes down the process, so readLocation
// has to report the corruption instead.
func TestReadLocationFieldIDOutOfRange(t *testing.T) {
	it := &PostingsIterator{
		postings:  &PostingsList{sb: &SegmentBase{fieldsInv: make([]string, 28)}},
		locReader: &chunkedIntDecoder{r: newMemUvarintReader(uvarints(281, 0, 0, 0, 0))},
	}

	err := it.readLocation(&Location{})
	if err == nil {
		t.Fatal("expected an error for an out-of-range field id, got nil")
	}
	if !strings.Contains(err.Error(), "281") {
		t.Errorf("expected the offending field id in %q", err)
	}
}

// The last valid field id must still decode.
func TestReadLocationFieldIDInRange(t *testing.T) {
	sb := &SegmentBase{fieldsInv: make([]string, 28)}
	sb.fieldsInv[27] = "lastField"

	it := &PostingsIterator{
		postings:  &PostingsList{sb: sb},
		locReader: &chunkedIntDecoder{r: newMemUvarintReader(uvarints(27, 1, 2, 3, 0))},
	}

	var l Location
	if err := it.readLocation(&l); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if l.field != "lastField" {
		t.Errorf("field = %q, want %q", l.field, "lastField")
	}
	if l.pos != 1 || l.start != 2 || l.end != 3 {
		t.Errorf("pos/start/end = %v/%v/%v, want 1/2/3", l.pos, l.start, l.end)
	}
}
