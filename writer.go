package ringbuf

import (
	"io"
	"net"
)

type BufferedWriter struct {
	buffer *Buffer
	err    error
	errCh  chan error
}

// NewWriter creates a BufferedWriter on top of a ringbuf of size. Data written to the BufferedWriter is sent to w in another thread.
// Close must be called to stop the writer goroutine.
func NewWriter(w io.Writer, size int) *BufferedWriter {
	b := &BufferedWriter{
		buffer: New(size),
		errCh:  make(chan error, 1),
	}
	go b.run(w)
	return b
}

// Write data to this BufferedWriter's destination. Write can fail if an _earlier_ write failed.
// Write may not be called concurrently with itself or Close.
func (b *BufferedWriter) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	select {
	case b.err = <-b.errCh:
		b.buffer.CloseWrite() // Allow run() to die.
		return 0, b.err
	default:
	}
	return b.buffer.Write(p)
}

// Close promises no more Write calls will happen.
// Close blocks until all data is flushed and returns the error from the writer.
// Close may not be called concurrently with itself or Write.
func (b *BufferedWriter) Close() error {
	if b.err != nil {
		return b.err
	}
	b.buffer.CloseWrite()
	if err := <-b.errCh; err != nil {
		b.err = err
		return err
	}
	b.err = net.ErrClosed
	return nil
}

func (b *BufferedWriter) run(w io.Writer) {
	_, err := b.buffer.WriteTo(w)
	b.errCh <- err
	if err != nil {
		// Drain the buffer to ensure Write() calls don't get stuck being blocked on a full buffer.
		b.buffer.WriteTo(io.Discard)
	}
}
