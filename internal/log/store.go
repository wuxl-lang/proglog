package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8
)

// The store struct is a simple wrapper around a file.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// File's current size
	// In case, it creates the store from a existed file.
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append given bytes to the store
// Return the number of bytes written and the position
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// Write the length of the record
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// Write content
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err // ? Clean written the length of record ?
	}

	w += lenWidth // total written byte
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read the record with a given position.
// ? Handle invalid position ?
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the writer buffer
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the length of the record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// Read the content
	b := make([]byte, enc.Uint64(size)) // Convert bytes to uint64
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// Read len(p) bytes into p beginning at the off offset in the store's file
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the wirter buffer
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close persists any bufferred data before closing the file
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the writter buffer
	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
