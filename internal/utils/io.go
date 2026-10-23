package utils

type ReaderFunc func([]byte) (int, error)

func (f ReaderFunc) Read(p []byte) (int, error) { return f(p) }

type CloserFunc func() error

func (f CloserFunc) Close() error { return f() }

type BoundedBuffer struct {
	Data  []byte
	Limit int
}

func (b *BoundedBuffer) Write(p []byte) (int, error) {
	b.Data = append(b.Data, p[:min(len(p), max(0, b.Limit-len(b.Data)))]...)

	return len(p), nil
}
