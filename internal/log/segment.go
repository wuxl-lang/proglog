package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/wuxl-lang/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// Construct segment with a dir and a base absolute offset
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	// Init segment
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	// Construct store
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Construct index
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Try to reload
	if off, _, err := s.index.Read(-1); err != nil { // index is empty
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1 // Add existed relative offset
	}

	return s, nil
}

// Append a record into store and return absolute offset
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// Assign next absolute offset to record
	cur := s.nextOffset
	record.Offset = cur

	// Marshal to bytes
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// Append bytes into store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// Write relative offset and position into index
	err = s.index.Write(uint32(cur-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}

	// Update the current absolute offset
	s.nextOffset++

	return cur, nil
}

// Read record by aboslute offset
func (s *segment) Read(off uint64) (*api.Record, error) {
	// Read position by relative offset
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Read record by position
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// New record
	record := &api.Record{}
	err = proto.Unmarshal(p, record)

	return record, err
}

// Store is full or index is full
func (s *segment) IsMax() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove store and index
// ? What if remove fail ?
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	return nil
}

// Close store and index
// ? What if close fail ?
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}
