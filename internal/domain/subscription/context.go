package subscription

// Context is the per-subscription mutable key-value bag exchanged with the
// integration service across each lifecycle action. Mirrors the contract
// of the upstream subscription service.
type Context map[string]string

// Clone returns an independent copy of the context (workflows should never
// share map references between runs).
func (c Context) Clone() Context {
	out := make(Context, len(c))
	for k, v := range c {
		out[k] = v
	}
	return out
}
