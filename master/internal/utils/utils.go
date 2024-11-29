package utils

import (
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
