package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/wuxl-lang/proglog/api/v1"
)

// A log structure contains a list of segments.
// Only one segment is active to append
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}

	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// Append record and return absolute offset
func (l *Log) Append(record *api.Record) (uint64, error) {
	// Exclude lock
	l.mu.Lock()
	defer l.mu.Unlock()

	// Append record to active segment
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// If active segment is ful, new a segment from
	if l.activeSegment.IsMax() {
		err = l.newSegment(off + 1)
	}

	return off, err
}

// Read record by absolute offset
func (l *Log) Read(off uint64) (*api.Record, error) {
	// Read lock
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment contains the absolute offset
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	if s == nil {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

// Close all segments
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Close all segments and remove the entire dir with persist store and index
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

// Removel the entire dir and reset log with initial offset
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}

	return l.setup()
}

// Lowest absolute offset
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.segments[0].baseOffset, nil
}

// Highest absolute offset
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Remove segment whose next offset -1 is less or equal than lowest
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, segment := range l.segments {
		if segment.nextOffset <= lowest+1 {
			if err := segment.Remove(); err != nil {
				return err
			}

			continue
		}
		segments = append(segments, segment)
	}
	l.segments = segments

	return nil
}

//Read the whole log
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0} // Construct a reader for each store
	}

	// concatenate readers
	return io.MultiReader(readers...)
}

// Reader interface to read data from store
type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off = int64(n)

	return n, err
}

// Load existing segments from a dir
func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Store is organized as baseOffset.store
	// Index is organized as baseoffset.index
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { // Sort existing baseOffsets
		return baseOffsets[i] < baseOffsets[j]
	})

	// Re-construct segment for each baseOffset
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}

		// baseOffset contains dup for index and store so it skip the dup
		i++
	}

	// If no existing segment, new segment from initial offset
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
