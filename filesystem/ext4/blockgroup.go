package ext4

import (
	"fmt"
)

// blockGroup is a structure holding the data about a single block group
type blockGroup struct {
	inodeBitmap    *bitmap
	blockBitmap    *bitmap
	blockSize      int
	number         int
	inodeTableSize int
	firstDataBlock int
}

// blockGroupFromBytes create a blockGroup struct from bytes
// it does not load the inode table or data blocks into memory, rather holding pointers to where they are
func blockGroupFromBytes(b []byte, blockSize, groupNumber int) (*blockGroup, error) {
	expectedSize := 2 * blockSize
	actualSize := len(b)
	if actualSize != expectedSize {
		return nil, fmt.Errorf("Expected to be passed %d bytes for 2 blocks of size %d, instead received %d", expectedSize, blockSize, actualSize)
	}
	inodeBitmap, err := bitmapFromBytes(b[0:blockSize])
	if err != nil {
		return nil, fmt.Errorf("Error creating inode bitmap from bytes: %v", err)
	}
	blockBitmap, err := bitmapFromBytes(b[blockSize : 2*blockSize])
	if err != nil {
		return nil, fmt.Errorf("Error creating block bitmap from bytes: %v", err)
	}

	bg := blockGroup{
		inodeBitmap: inodeBitmap,
		blockBitmap: blockBitmap,
		number:      groupNumber,
		blockSize:   blockSize,
	}
	return &bg, nil
}

// toBytes returns bitmaps ready to be written to disk
func (bg *blockGroup) toBytes() ([]byte, error) {
	b := make([]byte, 2*bg.blockSize)
	inodeBitmapBytes, err := bg.inodeBitmap.toBytes()
	if err != nil {
		return nil, fmt.Errorf("Error retrieving inode bitmap bytes: %v", err)
	}
	blockBitmapBytes, err := bg.blockBitmap.toBytes()
	if err != nil {
		return nil, fmt.Errorf("Error retrieving block bitmap bytes: %v", err)
	}

	b = append(b, inodeBitmapBytes...)
	b = append(b, blockBitmapBytes...)

	return b, nil
}
