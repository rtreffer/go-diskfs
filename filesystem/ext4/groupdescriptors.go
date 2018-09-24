package ext4

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type blockGroupFlag uint16
type gdtChecksumType uint8

const (
	groupDescriptorSize                    int             = 32
	groupDescriptorSize64Bit               int             = 64
	blockGroupFlagInodesUninitialized      blockGroupFlag  = 0x1
	blockGroupFlagBlockBitmapUninitialized blockGroupFlag  = 0x2
	blockGroupFlagInodeTableZeroed         blockGroupFlag  = 0x3
	gdtChecksumNone                        gdtChecksumType = 0
	gdtChecksumGdt                         gdtChecksumType = 1
	gdtChecksumMetadata                    gdtChecksumType = 2
)

type blockGroupFlags struct {
	inodesUninitialized      bool
	blockBitmapUninitialized bool
	inodeTableZeroed         bool
}

// groupdescriptors is a structure holding all of the group descriptors for all of the block groups
type groupDescriptors struct {
	descriptors []groupDescriptor
}

// groupDescriptor is a structure holding the data about a single block group
type groupDescriptor struct {
	blockBitmapLocation             uint64
	inodeBitmapLocation             uint64
	inodeTableLocation              uint64
	freeBlocks                      uint32
	freeInodes                      uint32
	usedDirectories                 uint32
	flags                           blockGroupFlags
	snapshotExclusionBitmapLocation uint64
	blockBitmapChecksum             uint32
	inodeBitmapChecksum             uint32
	unusedInodes                    uint32
	is64bit                         bool
	number                          uint64
}

func (gd *groupDescriptors) equal(a *groupDescriptors) bool {
	if (gd == nil && a != nil) || (a == nil && gd != nil) {
		return false
	}
	if gd == nil && a == nil {
		return true
	}
	return *gd == *a
}

// groupDescriptorsFromBytes create a groupDescriptors struct from bytes
func groupDescriptorsFromBytes(b []byte, is64bit bool, superblockUuid []byte, checksumType gdtChecksumType) (*groupDescriptors, error) {
	gds := groupDescriptors{}
	gdSlice := make([]groupDescriptor, 10)

	gdSize := groupDescriptorSize
	if is64bit {
		gdSize = groupDescriptorSize64Bit
	}
	count := len(b) / gdSize

	// go through them gdSize bytes at a time
	for i := 0; i < count; i++ {
		start := i * gdSize
		end := start + gdSize
		gdSlice = append(gdSlice, groupDescriptorFromBytes(b[start:end], is64bit, i, checksumType, superblockUuid))
	}
	gds.descriptors = gdSlice

	return &gds, nil
}

// toBytes returns groupDescriptors ready to be written to disk
func (gds *groupDescriptors) toBytes(checksumType gdtChecksumType, superblockUuid []byte) ([]byte, error) {
	b := make([]byte, 10*groupDescriptorSize)
	for _, gd := range gds.descriptors {
		b = append(b, gd.toBytes(checksumType, superblockUuid)...)
	}

	return b, nil
}

// groupDescriptorFromBytes create a groupDescriptor struct from bytes
func groupDescriptorFromBytes(b []byte, is64bit bool, number int, checksumType gdtChecksumType, superblockUuid []byte) (*groupDescriptor, error) {
	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	blockBitmapLocation := make([]byte, 8, 8)
	inodeBitmapLocation := make([]byte, 8, 8)
	inodeTableLocation := make([]byte, 8, 8)
	freeBlocks := make([]byte, 4, 4)
	freeInodes := make([]byte, 4, 4)
	usedirectories := make([]byte, 4, 4)
	snapshotExclusionBitmapLocation := make([]byte, 8, 8)
	blockBitmapChecksum := make([]byte, 4, 4)
	inodeBitmapChecksum := make([]byte, 4, 4)
	unusedInodes := make([]byte, 4, 4)

	copy(blockBitmapLocation[0:4], b[0x0:0x4])
	copy(inodeBitmapLocation[0:4], b[0x4:0x8])
	copy(inodeTableLocation[0:4], b[0x8:0xc])
	copy(freeBlocks[0:2], b[0xc:0xe])
	copy(freeInodes[0:2], b[0xe:0x10])
	copy(usedirectories[0:2], b[0x10:0x12])
	copy(snapshotExclusionBitmapLocation[0:4], b[0x14:0x18])
	copy(blockBitmapChecksum[0:2], b[0x18:0x1a])
	copy(inodeBitmapChecksum[0:2], b[0x1a:0x1c])
	copy(unusedInodes[0:2], b[0x1c:0x1e])

	if is64bit {
		copy(blockBitmapLocation[4:8], b[0x20:0x24])
		copy(inodeBitmapLocation[4:8], b[0x24:0x28])
		copy(inodeTableLocation[4:8], b[0x28:0x2c])
		copy(freeBlocks[2:4], b[0x2c:0x2e])
		copy(freeInodes[2:4], b[0x2e:0x30])
		copy(usedirectories[2:4], b[0x30:0x32])
		copy(unusedInodes[2:4], b[0x32:0x34])
		copy(snapshotExclusionBitmapLocation[4:8], b[0x34:0x38])
		copy(blockBitmapChecksum[2:4], b[0x38:0x3a])
		copy(inodeBitmapChecksum[2:4], b[0x3a:0x3c])
	}

	gdNumber := uint64(number)
	// only bother with checking the checksum if it was not type none (pre-checksums)
	if checksumType != gdtChecksumNone {
		checksum := binary.LittleEndian.Uint16(b[0x1e:0x20])
		actualChecksum := groupDescriptorChecksum(b[0x0:0x1e], superblockUuid, gdNumber, checksumType)
		if checksum != actualChecksum {
			return nil, fmt.Errorf("checksum mismatch, passed %x, actual %x", checksum, actualChecksum)
		}
	}

	gd := groupDescriptor{
		is64bit:                         is64bit,
		number:                          gdNumber,
		blockBitmapLocation:             binary.LittleEndian.Uint64(blockBitmapLocation),
		inodeBitmapLocation:             binary.LittleEndian.Uint64(inodeBitmapChecksum),
		inodeTableLocation:              binary.LittleEndian.Uint64(inodeTableLocation),
		freeBlocks:                      binary.LittleEndian.Uint32(freeBlocks),
		freeInodes:                      binary.LittleEndian.Uint32(freeInodes),
		usedDirectories:                 binary.LittleEndian.Uint32(usedirectories),
		snapshotExclusionBitmapLocation: binary.LittleEndian.Uint64(snapshotExclusionBitmapLocation),
		blockBitmapChecksum:             binary.LittleEndian.Uint32(blockBitmapChecksum),
		inodeBitmapChecksum:             binary.LittleEndian.Uint32(inodeBitmapChecksum),
		unusedInodes:                    binary.LittleEndian.Uint32(unusedInodes),
		flags:                           parseBlockGroupFlags(binary.LittleEndian.Uint16(b[0x12:0x14])),
	}

	return &gd, nil
}

// toBytes returns a groupDescriptor ready to be written to disk
func (gd *groupDescriptor) toBytes(checksumType gdtChecksumType, superblockUuid []byte) ([]byte, error) {
	gdSize := groupDescriptorSize

	// size of byte slice returned depends upon if using 64bit or 32bit filesystem
	if gd.is64bit {
		gdSize = groupDescriptorSize64Bit
	}
	b := make([]byte, gdSize, gdSize)

	blockBitmapLocation := make([]byte, 8, 8)
	inodeBitmapLocation := make([]byte, 8, 8)
	inodeTableLocation := make([]byte, 8, 8)
	freeBlocks := make([]byte, 4, 4)
	freeInodes := make([]byte, 4, 4)
	usedirectories := make([]byte, 4, 4)
	snapshotExclusionBitmapLocation := make([]byte, 8, 8)
	blockBitmapChecksum := make([]byte, 4, 4)
	inodeBitmapChecksum := make([]byte, 4, 4)
	unusedInodes := make([]byte, 4, 4)

	binary.LittleEndian.PutUint64(blockBitmapLocation, gd.blockBitmapLocation)
	binary.LittleEndian.PutUint64(inodeTableLocation, gd.inodeTableLocation)
	binary.LittleEndian.PutUint64(inodeBitmapLocation, gd.inodeBitmapLocation)
	binary.LittleEndian.PutUint32(freeBlocks, gd.freeBlocks)
	binary.LittleEndian.PutUint32(freeInodes, gd.freeInodes)
	binary.LittleEndian.PutUint32(usedirectories, gd.usedDirectories)
	binary.LittleEndian.PutUint64(snapshotExclusionBitmapLocation, gd.snapshotExclusionBitmapLocation)
	binary.LittleEndian.PutUint32(blockBitmapChecksum, gd.blockBitmapChecksum)
	binary.LittleEndian.PutUint32(inodeBitmapChecksum, gd.inodeBitmapChecksum)
	binary.LittleEndian.PutUint32(unusedInodes, gd.unusedInodes)

	// copy the lower 32 bytes in
	copy(b[0x0:0x4], blockBitmapLocation[0:4])
	copy(b[0x4:0x8], inodeBitmapLocation[0:4])
	copy(b[0x8:0xc], inodeTableLocation[0:4])
	copy(b[0xc:0xe], freeBlocks[0:2])
	copy(b[0xe:0x10], freeInodes[0:2])
	copy(b[0x10:0x12], usedirectories[0:2])
	binary.LittleEndian.PutUint16(b[0x12:0x14], gd.flags.toInt())
	copy(b[0x14:0x18], snapshotExclusionBitmapLocation[0:4])
	copy(b[0x18:0x1a], blockBitmapChecksum[0:2])
	copy(b[0x1a:0x1c], inodeBitmapChecksum[0:2])
	copy(b[0x1c:0x1e], unusedInodes[0:2])

	// now for the upper 32 bytes
	if gd.is64bit {
		copy(b[0x20:0x24], blockBitmapLocation[4:8])
		copy(b[0x24:0x28], inodeBitmapLocation[4:8])
		copy(b[0x28:0x2c], inodeTableLocation[4:8])
		copy(b[0x2c:0x2e], freeBlocks[2:4])
		copy(b[0x2e:0x30], freeInodes[2:4])
		copy(b[0x30:0x32], usedirectories[2:4])
		copy(b[0x32:0x34], unusedInodes[2:4])
		copy(b[0x34:0x38], snapshotExclusionBitmapLocation[4:8])
		copy(b[0x38:0x3a], blockBitmapChecksum[2:4])
		copy(b[0x3a:0x3c], inodeBitmapChecksum[2:4])
	}

	checksum := groupDescriptorChecksum(b[0x0:0x1e], superblockUuid, gd.number, checksumType)
	binary.LittleEndian.PutUint16(b[0x1e:0x20], checksum)

	return b, nil
}

func parseBlockGroupFlags(flags uint16) blockGroupFlags {
	f := blockGroupFlags{
		inodeTableZeroed:         flags&blockGroupFlagInodeTableZeroed == blockGroupFlagInodeTableZeroed,
		inodesUninitialized:      flags&blockGroupFlagInodesUninitialized == blockGroupFlagInodesUninitialized,
		blockBitmapUninitialized: flags&blockGroupFlagBlockBitmapUninitialized == blockGroupFlagBlockBitmapUninitialized,
	}

	return f
}

func (f *blockGroupFlags) toInt() uint16 {
	var (
		flags uint16
	)

	// compatible flags
	if f.inodeTableZeroed {
		flags = flags | blockGroupFlagInodeTableZeroed
	}
	if f.inodesUninitialized {
		flags = flags | blockGroupFlagInodesUninitialized
	}
	if f.blockBitmapUninitialized {
		flags = flags | blockGroupFlagBlockBitmapUninitialized
	}
	return flags
}

// groupDescriptorChecksum calculate the checksum for a block group descriptor
// NOTE: we are assuming that the block group number is uint64, but we do not know that to be true
//    it might be uint32 or uint64, and it might be in BigEndian as opposed to LittleEndian
//    just have to start with this and see
//    we do know that the maximum number of block groups in 32-bit mode is 2^19, which must be uint32
//    and in 64-bit mode it is 2^51 which must be uint64
//    So we start with uint32 = [4]byte{} for regular mode and [8]byte{} for mod32
func groupDescriptorChecksum(b, superblockUuid []byte, groupNumber uint64, checksumType gdtChecksumType) uint16 {
	var checksum uint16
	var input []byte

	groupBytes := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(groupBytes, groupNumber)
	switch checksumType {
	case gdtChecksumNone:
		checksum = 0
	case gdtChecksumMetadata:
		input = append(superblockUuid, groupBytes, b...)
		crc32Table := crc32.MakeTable(crc32.Castagnoli)
		checksum32 := crc32.Checksum(input, crc32Table)
		checksum = checksum32 & 0xffff
	case gdtChecksumGdt:
		input = append(superblockUuid, groupBytes[0:4], b...)
		checksum = crc16(input)
	}
	return checksum
}
