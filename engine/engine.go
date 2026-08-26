package engine

// Copy semantics: Put copies value before storing it, and Get returns a
// copy of the stored value. Callers may freely mutate the slice they
// passed to Put, or the slice returned by Get, without affecting the
// engine's internal state (or racing with it).
type Engine interface {
	Get(key string) (value []byte, ok bool)
	Put(key string, value []byte)
	Delete(key string)
}
