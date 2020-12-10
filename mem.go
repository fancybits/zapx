//  Copyright (c) 2017 Couchbase, Inc.
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
	"io"
	"io/ioutil"
	"log"
	"sync/atomic"

	mmap "github.com/blevesearch/mmap-go"
)

var (
	mmapCurrentBytes int64

	// Ignore mmap failures and fallback to regular file access.
	MmapIgnoreErrors bool

	// Optional, maximum number of bytes to mmap before fallback to regular file access.
	MmapMaxBytes int64
)

type zapStats struct {
	// Total number of bytes mapped into memory with mmap.
	MmapCurrentBytes int64
}

type mmapOwner interface {
	readMM(start, end uint64) []byte
}

// Stats returns memory usage details for open zap segments in this process.
func Stats() zapStats {
	return zapStats{
		MmapCurrentBytes: atomic.LoadInt64(&mmapCurrentBytes),
	}
}

func (s *SegmentBase) readMem(start, end uint64) []byte {
	if s.mmapOwner != nil {
		return s.mmapOwner.readMM(start, end)
	}
	return s.mem[start:end]
}

func (s *Segment) readMM(start, end uint64) []byte {
	s.mmLock.RLock()
	mm := s.mm
	s.mmLock.RUnlock()

	if mm != nil {
		return mm[start:end]
	}

	data, _ := ioutil.ReadAll(io.NewSectionReader(s.f, int64(start), int64(end-start)))
	return data
}

func (s *Segment) loadMmap() error {
	if MmapMaxBytes > 0 &&
		atomic.LoadInt64(&mmapCurrentBytes)+int64(s.mmSize) > MmapMaxBytes {
		return nil
	}

	if s.mm != nil {
		return nil
	}

	s.mmLock.Lock()
	defer s.mmLock.Unlock()

	if s.mm != nil {
		return nil
	}

	mm, err := mmap.Map(s.f, mmap.RDONLY, 0)
	if err == nil {
		atomic.AddInt64(&mmapCurrentBytes, int64(s.mmSize))
	} else if MmapIgnoreErrors {
		log.Printf("[WRN] Ignoring mmap error: %v", err)
		return nil
	} else {
		return err
	}

	s.mm = mm
	return nil
}

func (s *Segment) unloadMmap() error {
	s.mmLock.Lock()
	defer s.mmLock.Unlock()

	if s.mm == nil {
		return nil
	}

	err := s.mm.Unmap()
	if err == nil {
		atomic.AddInt64(&mmapCurrentBytes, -int64(s.mmSize))
		s.mm = nil
	}
	return err
}

// MmapHint requests that the segment be mapped into memory for faster access.
//
// This usually happens automatically, but may be necessary on 32-bit systems or when
// using MmapMaxBytes.
func (s *Segment) MmapHint() {
	s.loadMmap()
}
