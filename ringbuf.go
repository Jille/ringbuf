// Package ringbuf provides a ring buffer of bytes that accepts a parallel reader and writer.
package ringbuf

import (
	"io"
	"sync"

	"github.com/Jille/easymutex"
)

// Buffer is a ring buffer that accepts a parallel reader and writer.
type Buffer struct {
	mtx        sync.Mutex
	cond       *sync.Cond
	data       []byte
	inboundPos int
	buffered   int
	eof        bool
}

var (
	_ io.Reader     = (*Buffer)(nil)
	_ io.WriterTo   = (*Buffer)(nil)
	_ io.Writer     = (*Buffer)(nil)
	_ io.ReaderFrom = (*Buffer)(nil)
)

func New(size int) *Buffer {
	b := &Buffer{
		data: make([]byte, size),
	}
	b.cond = sync.NewCond(&b.mtx)
	return b
}

func (b *Buffer) getSliceForInbound() []byte {
	for b.buffered == len(b.data) {
		b.cond.Wait()
	}
	if b.buffered == 0 {
		// Reset the inboundPos so we can do maximum size Read()s again.
		b.inboundPos = 0
		return b.data
	}
	end := b.inboundPos - b.buffered
	if end < 0 {
		return b.data[b.inboundPos : len(b.data)+end]
	} else {
		return b.data[b.inboundPos:]
	}
}

func (b *Buffer) getSliceForOutbound() ([]byte, bool) {
	for b.buffered == 0 {
		if b.eof {
			return nil, true
		}
		b.cond.Wait()
	}
	offset := b.inboundPos - b.buffered
	if offset < 0 {
		return b.data[len(b.data)+offset:], false
	} else {
		return b.data[offset:b.inboundPos], false
	}
}

// Write always returns len(p), nil.
func (b *Buffer) Write(p []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	ret := len(p)
	for len(p) > 0 {
		buf := b.getSliceForInbound()
		n := copy(buf, p)
		b.inboundPos = (b.inboundPos + n) % len(b.data)
		b.buffered += n
		p = p[n:]
		b.cond.Signal()
	}
	return ret, nil
}

// ReadFrom does not return any errors besides those from the given Reader.
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	em := easymutex.LockMutex(&b.mtx)
	defer em.Unlock()
	var read int
	for {
		buf := b.getSliceForInbound()
		em.Unlock()
		n, err := r.Read(buf)
		em.Lock()
		read += n
		b.inboundPos = (b.inboundPos + n) % len(b.data)
		b.buffered += n
		b.cond.Signal()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return int64(read), err
		}
	}
}

// CloseWrite promises no more Write/ReadFrom calls will happen and lets Read return EOF.
// CloseWriter always returns a nil error.
func (b *Buffer) CloseWrite() error {
	b.mtx.Lock()
	b.eof = true
	b.mtx.Unlock()
	return nil
}

// WriteTo does not return any errors besides those from the given Writer.
func (b *Buffer) WriteTo(w io.Writer) (int64, error) {
	em := easymutex.LockMutex(&b.mtx)
	defer em.Unlock()
	maxWrite := len(b.data) / 4
	var written int
	for {
		ready, eof := b.getSliceForOutbound()
		if eof {
			return int64(written), nil
		}
		em.Unlock()
		if len(ready) > maxWrite {
			// Use smaller writes so we free up regions of the buffer faster if w is blocking.
			ready = ready[:maxWrite]
		}
		n, err := w.Write(ready)
		em.Lock()
		written += n
		b.buffered -= n
		b.cond.Signal()
		if err != nil {
			return int64(written), err
		}
	}
}

// Read always returns a nil error or io.EOF.
func (b *Buffer) Read(p []byte) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	ready, eof := b.getSliceForOutbound()
	if eof {
		return 0, io.EOF
	}
	n := copy(p, ready)
	b.buffered -= n
	b.cond.Signal()
	return n, nil
}

// FreeBytes returns the number of bytes that can be safely written into the Buffer without blocking.
// If called concurrently with a Write/ReadFrom call, the answer might be stale by the time this function returns.
func (b *Buffer) FreeBytes() int {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return len(b.data) - b.buffered
}
