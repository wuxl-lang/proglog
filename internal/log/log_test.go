package log

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/wuxl-lang/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

var test_record = &api.Record{
	Value: []byte("Hello World!"),
}

func TestLog(t *testing.T) {
	cases := map[string]func(t *testing.T, log *Log){
		"append and read":   testAppendRead,
		"init existing log": testInitExisting,
		"reader":            testReader,
		"truncate":          testTruncate,
	}

	fmt.Printf("test\n")
	for scenario, fn := range cases {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "store-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	off, err := log.Append(test_record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off) // First record

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, test_record.Value, read.Value)

	// Read out of range
	read, err = log.Read(off + 1)
	require.Error(t, err)
	require.Nil(t, read)

	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, off+1, apiErr.Offset)
}

func testInitExisting(t *testing.T, log *Log) {
	for i := 0; i < 3; i++ {
		off, err := log.Append(test_record)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}

	require.NoError(t, log.Close())

	l, err := NewLog(log.Dir, Config{})
	require.NoError(t, err)

	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)
}

func testReader(t *testing.T, log *Log) {
	off, err := log.Append(test_record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read) // skip record length
	require.NoError(t, err)
	require.Equal(t, test_record.Value, read.Value)
}

func testTruncate(t *testing.T, log *Log) {
	fmt.Printf("%d", len(log.segments))

	for i := 0; i < 3; i++ {
		off, err := log.Append(test_record)
		require.NoError(t, err)
		require.Equal(t, uint64(i), off)
	}

	require.Equal(t, 2, len(log.segments))                  // Has two segment
	require.Equal(t, uint64(2), log.segments[0].nextOffset) // First segment has two elements

	log.Truncate(1) // First segment is removed
	require.Equal(t, 1, len(log.segments))
}
