package kv

import "io"

type BatchReaderFunc func(Reader) error

type BatchReadWriterFunc func(ReadWriter) error

type Service interface {
	ReadWriter
	Batcher
}

type ReadWriter interface {
	Reader
	Writer
}

type Reader interface {
	Has(key []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	NewIterator(lower, upper []byte) Iterator
}

type Writer interface {
	Set(key, value []byte) error
	Delete(key []byte) error
}

type Batcher interface {
	ROBatch(BatchReaderFunc) error
	RWBatch(BatchReadWriterFunc) error
}

type Iterator interface {
	Valid() bool
	Next() bool
	First() bool
	Key() []byte
	Value() []byte
	io.Closer
}
