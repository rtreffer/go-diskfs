package ext4

import (
	"encoding/binary"
	"fmt"
)

const (
	minDirEntryLength int = 12 // actually 9 for 1-byte file length, but must be multiple of 4 bytes
	maxDirEntryLength int = 263
)

// directoryEntry is a single directory entry
type directoryEntry struct {
	inode    uint32
	filename string
	fileType fileType
}

func directoryEntryFromBytes(b []byte) (*directoryEntry, error) {
	if len(b) < minDirEntryLength {
		return nil, fmt.Errorf("directory entry of length %d is less than minimum %d", len(b), minDirEntryLength)
	}
	if len(b) > maxDirEntryLength {
		return nil, fmt.Errorf("directory entry of length %d is greater than maximum %d", len(b), maxDirEntryLength)
	}

	length := binary.LittleEndian.Uint16(b[0x4:0x6])
	nameLength := uint8(b[0x6])
	name := b[0x8 : 0x8+nameLength]
	de := directoryEntry{
		inode:    binary.LittleEndian.Uint32(b[0x0:0x4]),
		fileType: fileType(b[0x7]),
		filename: string(name),
	}
	return &de, nil
}

func (de *directoryEntry) toBytes() ([]byte, error) {
	// it must be the header length + filename length rounded up to nearest multiple of 4
	nameLength := uint8(len(de.filename))
	entryLength := uint16(nameLength) + 8
	if leftover := entryLength % 4; leftover > 0 {
		entryLength += leftover
	}
	b := make([]byte, 0, entryLength)

	binary.LittleEndian.PutUint32(b[0x0:0x4], de.inode)
	binary.LittleEndian.PutUint16(b[0x4:0x6], entryLength)
	b[0x6] = nameLength
	b[0x7] = byte(de.fileType)
	copy(b[0x8:], []byte(de.filename))

	return b, nil
}

// parse the data blocks to get the directory entries
func parseDirEntries(b []byte, f *FileSystem) ([]*directoryEntry, error) {
	entries := make([]*directoryEntry, 4)
	count := 0
	for i := 0; i < len(b); count++ {
		// read the length of the first entry
		length := binary.LittleEndian.Uint16(b[i+0x4 : i+0x6])
		de, err := directoryEntryFromBytes(b[i : i+int(length)])
		if err != nil {
			return nil, fmt.Errorf("Failed to parse directory entry %d: %v", count, err)
		}
		entries = append(entries, de)
		i += int(length)
	}
	return entries, nil
}
