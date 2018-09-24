package ext4

// Directory represents a single directory in an ext4 filesystem
type Directory struct {
	directoryEntry
	root    bool
	entries []*directoryEntry
}

// dirEntriesFromBytes loads the directory entries from the raw bytes
func (d *Directory) entriesFromBytes(b []byte, f *FileSystem) error {
	entries, err := parseDirEntries(b, f)
	if err != nil {
		return err
	}
	d.entries = entries
	return nil
}

// toBytes convert our entries to raw bytes
func (d *Directory) toBytes(bytesPerBlock int) ([]byte, error) {
	b := make([]byte, 0)
	for _, de := range d.entries {
		b2, err := de.toBytes()
		if err != nil {
			return nil, err
		}
		b = append(b, b2...)
	}
	remainder := len(b) % bytesPerBlock
	extra := bytesPerBlock - remainder
	zeroes := make([]byte, extra, extra)
	b = append(b, zeroes...)
	return b, nil
}
