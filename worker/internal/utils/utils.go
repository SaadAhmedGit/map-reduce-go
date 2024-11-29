package utils

import (
	"os"
	"sync"
)

// SafeSlice is a thread-safe slice.
type SafeSlice[T any] struct {
	slice []T
	mutex sync.RWMutex
}

// NewSafeSlice creates a new instance of SafeSlice.
func NewSafeSlice[T any]() *SafeSlice[T] {
	return &SafeSlice[T]{
		slice: make([]T, 0),
	}
}

// Append adds an element to the slice.
func (ss *SafeSlice[T]) Append(value T) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()
	ss.slice = append(ss.slice, value)
}

func (ss *SafeSlice[T]) Len() int {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()
	return len(ss.slice)
}

func (ss *SafeSlice[T]) Get(index int) *T {
	ss.mutex.RLock()
	defer ss.mutex.RUnlock()
	return &ss.slice[index]
}

// SafeFile is a thread-safe file.
type SafeFile struct {
	file  *os.File
	mutex sync.RWMutex
}

// NewSafeFile creates a new instance of SafeFile.
func NewSafeFile(filePath string) (*SafeFile, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return &SafeFile{
		file: file,
	}, nil
}

func NewTempSafeFile(prefix string) (*SafeFile, error) {
	file, err := os.CreateTemp("", prefix)
	if err != nil {
		return nil, err
	}
	return &SafeFile{
		file: file,
	}, nil
}

func (sf *SafeFile) Write(data []byte) (int, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	return sf.file.Write(data)
}

func (sf *SafeFile) Close() error {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	return sf.file.Close()
}

func (sf *SafeFile) Sync() error {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	return sf.file.Sync()
}

func (sf *SafeFile) ReadChunk(chunkSize int) ([]byte, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()
	chunk := make([]byte, chunkSize)
	_, err := sf.file.Read(chunk)
	return chunk, err
}
