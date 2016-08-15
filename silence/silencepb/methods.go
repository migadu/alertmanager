package silencepb

func (s *Silence) Merge(o *Silence) {
	if o == nil {
		return
	}
}

func (s *Silence) Expired() bool {
	panic("not implemnented")
}
