package log

import (
	"io"
	"os"

	"github.com/tysontate/gommap"
)

var (
	// Size of offset
	offWidth uint64 = 4
	// Size of position
	posWidth uint64 = 8
	// Size of an entry contains offset and position
	entWidth = offWidth + posWidth
)

// Index comprsies a persisted file and a memory-mapped file.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Once they’re memory-mapped, we can’t resize them, so it’s now or never.
	// Grow the file to the max index size before memory-mapping the file
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// The input is relative to the segment's base offset;
// 0 is always the offset of the index's first entry
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Append offset and posititon into Index
func (i *index) Write(off uint32, pos uint64) error {
	// No enough space to add new entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	// Sync memory-mapped file to the persisted file
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// Flush persisted file to storage
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Trucates the persisted file to the amount of data that's actually in it.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return nil
	}

	return i.file.Close()
}
