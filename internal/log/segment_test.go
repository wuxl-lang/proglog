package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/wuxl-lang/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	// Create a temp directory
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{
		Value: []byte("Hello World!"),
	}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3 // Only 3 entries in index

	baseOffset := uint64(16)
	s, err := newSegment(dir, baseOffset, c) // Base absolute offset
	require.NoError(t, err)
	require.Equal(t, baseOffset, s.nextOffset, s.nextOffset) // Empty index
	require.False(t, s.IsMax())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want) // Store is appended but index is full
	require.Equal(t, io.EOF, err)

	require.True(t, s.IsMax()) // Index is full

	// Reload with small max store bytes config
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.True(t, s.IsMax())

	// Clean
	err = s.Remove()
	require.NoError(t, err)

	// As segment is removed, it should be empty
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMax())

}
