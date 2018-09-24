package ext4

// extens a structure holding multiple extents
type extents struct {
	extents []extent
}

// extent a structure with information about a contiguous run of blocks
type extent struct {
	fileBlock     uint32
	startingBlock uint64
	count         uint16
}

func (e *extent) equal(a *extent) bool {
	if (e == nil && a != nil) || (a == nil && e != nil) {
		return false
	}
	if e == nil && a == nil {
		return true
	}
	return *e == *a
}

func (e *extents) blocks() uint64 {
	var count uint64
	for _, ext := range e.extents {
		count += ext.count
	}
	return count
}
