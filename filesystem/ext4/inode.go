package ext4

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
)

type inodeFlag uint32
type fileType uint16

const (
	inodeSize                        int       = 128
	extentTreeHeaderLength           int       = 12
	extentTreeEntryLength            int       = 12
	extentHeaderSignature            uint16    = 0xf30a
	extentTreeMaxDepth               int       = 5
	extentInodeMaxEntries            int       = 4
	inodeFlagSecureDeletion          inodeFlag = 0x1
	inodeFlagPreserveForUndeletion   inodeFlag = 0x2
	inodeFlagCompressed              inodeFlag = 0x4
	inodeFlagSynchronous             inodeFlag = 0x8
	inodeFlagImmutable               inodeFlag = 0x10
	inodeFlagAppendOnly              inodeFlag = 0x20
	inodeFlagNoDump                  inodeFlag = 0x40
	inodeFlagNoAccessTimeUpdate      inodeFlag = 0x80
	inodeFlagDirtyCompressed         inodeFlag = 0x100
	inodeFlagCompressedClusters      inodeFlag = 0x200
	inodeFlagNoCompress              inodeFlag = 0x400
	inodeFlagEncryptedInode          inodeFlag = 0x800
	inodeFlagHashedDirectoryIndexes  inodeFlag = 0x1000
	inodeFlagAFSMagicDirectory       inodeFlag = 0x2000
	inodeFlagAlwaysJournal           inodeFlag = 0x4000
	inodeFlagNoMergeTail             inodeFlag = 0x8000
	inodeFlagSyncDirectoryData       inodeFlag = 0x10000
	inodeFlagTopDirectory            inodeFlag = 0x20000
	inodeFlagHugeFile                inodeFlag = 0x40000
	inodeFlagUsesExtents             inodeFlag = 0x80000
	inodeFlagExtendedAttributes      inodeFlag = 0x200000
	inodeFlagBlocksPastEOF           inodeFlag = 0x400000
	inodeFlagSnapshot                inodeFlag = 0x1000000
	inodeFlagDeletingSnapshot        inodeFlag = 0x4000000
	inodeFlagCompletedSnapshotShrink inodeFlag = 0x8000000
	inodeFlagInlineData              inodeFlag = 0x10000000
	inodeFlagInheritProject          inodeFlag = 0x20000000

	fileTypeFifo            fileType = 0x1000
	fileTypeCharacterDevice fileType = 0x2000
	fileTypeDirectory       fileType = 0x4000
	fileTypeBlockDevice     fileType = 0x6000
	fileTypeRegularFile     fileType = 0x8000
	fileTypeSymbolicLink    fileType = 0xA000
	fileTypeSocket          fileType = 0xC000

	filePermissionsOwnerExecute uint16 = 0x40
	filePermissionsOwnerWrite   uint16 = 0x80
	filePermissionsOwnerRead    uint16 = 0x100
	filePermissionsGroupExecute uint16 = 0x8
	filePermissionsGroupWrite   uint16 = 0x10
	filePermissionsGroupRead    uint16 = 0x20
	filePermissionsOtherExecute uint16 = 0x1
	filePermissionsOtherWrite   uint16 = 0x2
	filePermissionsOtherRead    uint16 = 0x4
)

// mountOptions is a structure holding flags for an inode
type inodeFlags struct {
	secureDeletion          bool
	preserveForUndeletion   bool
	compressed              bool
	synchronous             bool
	immutable               bool
	appendOnly              bool
	noDump                  bool
	noAccessTimeUpdate      bool
	dirtyCompressed         bool
	compressedClusters      bool
	noCompress              bool
	encryptedInode          bool
	hashedDirectoryIndexes  bool
	AFSMagicDirectory       bool
	alwaysJournal           bool
	noMergeTail             bool
	syncDirectoryData       bool
	topDirectory            bool
	hugeFile                bool
	usesExtents             bool
	extendedAttributes      bool
	blocksPastEOF           bool
	snapshot                bool
	deletingSnapshot        bool
	completedSnapshotShrink bool
	inlineData              bool
	inheritProject          bool
}

type filePermissions struct {
	read    bool
	write   bool
	execute bool
}

// extentTree represents a tree of extents in an inode
//  it could be represented just as extents or []extent
//  but that would require recreation of the tree every time, which is a mess
type extentTree struct {
	depth       uint16        // the depth of tree below here. 0 means now children trees, all extents
	entries     uint16        // number of entries, either extents or children
	max         uint16        // maximum number of entries - children or extents - allowed at this level
	blockNumber uint64        // block number where the children live
	fileBlock   uint32        // extents or children of this cover from file block fileBlock onwards
	extents     extents       // for depth = 0, the actual extents; for depth > 0, empty
	children    []*extentTree // for depth = 0, empty; for depth > 0, the children
}

// inode is a structure holding the data about an inode
type inode struct {
	number                      uint64
	permissionsOther            filePermissions
	permissionsGroup            filePermissions
	permissionsOwner            filePermissions
	fileType                    fileType
	owner                       uint32
	group                       uint32
	size                        uint64
	accessTimeSeconds           int64
	changeTimeSeconds           int64
	creationTimeSeconds         int64
	modificationTimeSeconds     int64
	accessTimeNanoseconds       uint32
	changeTimeNanoseconds       uint32
	creationTimeNanoseconds     uint32
	modificationTimeNanoseconds uint32
	deletionTime                uint32
	hardLinks                   uint16
	blocks                      uint64
	filesystemBlocks            bool
	flags                       *inodeFlags
	version                     uint64
	nfsFileVersion              uint32
	extendedAttributeBlock      uint64
	inodeSize                   uint16
	project                     uint32
	extents                     *extentTree
}

func (i *inode) equal(a *inode) bool {
	if (i == nil && a != nil) || (a == nil && i != nil) {
		return false
	}
	if i == nil && a == nil {
		return true
	}
	return *i == *a
}

// inodeFromBytes create an inode struct from bytes
func inodeFromBytes(b []byte, sb *superblock, number int64) (*inode, error) {
	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	owner := make([]byte, 4, 4)
	fileSize := make([]byte, 8, 8)
	group := make([]byte, 4, 4)
	accessTime := make([]byte, 8, 8)
	changeTime := make([]byte, 8, 8)
	modifyTime := make([]byte, 8, 8)
	createTime := make([]byte, 8, 8)
	version := make([]byte, 8, 8)
	extendedAttributeBlock := make([]byte, 8, 8)
	checksumBytes := make([]byte, 4, 4)

	// checksum before using the data
	copy(checksumBytes[0:2], b[0x7c:0x7e])
	copy(checksumBytes[2:4], b[0x82:0x84])
	// zero out checksum fields
	b[0x7c] = 0
	b[0x7d] = 0
	b[0x82] = 0
	b[0x83] = 0

	checksum := binary.LittleEndian.Uint32(checksumBytes)
	actualChecksum := inodeChecksum(b, sb.uuid, number)

	if actualChecksum != checksum {
		return nil, fmt.Errorf("Checksum mismatch, on-disk %x vs calculated %x", checksum, actualChecksum)
	}

	mode := binary.LittleEndian.Uint16(b[0x0:0x2])

	copy(owner[0:2], b[0x2:0x4])
	copy(owner[2:4], b[0x78:0x7a])
	copy(group[0:2], b[0x18:0x20])
	copy(group[2:4], b[0x7a:0x7c])
	copy(size[0:4], b[0x4:0x8])
	copy(size[4:8], b[0x6c:0x70])
	copy(version[0:4], b[0x24:0x28])
	copy(version[4:8], b[0x98:0x9c])
	copy(extendedAttributeBlock[0:4], b[0x88:0x8c])
	copy(extendedAttributeBlock[4:6], b[0x76:0x78])

	// get the the times
	// the structure is as follows:
	//  original 32 bits (0:4) are seconds. Add (to the left) 2 more bits from the 32
	//  the remaining 30 bites are nanoseconds
	copy(accessTime[0:4], b[0x8:0xc])
	// take the two bits relevant and add to fifth byte
	accessTime[4] = b[0x8c] & 0x3
	copy(changeTime[0:4], b[0xc:0x10])
	changeTime[4] = b[0x84] & 0x3
	copy(modifyTime[0:4], b[0x10:0x14])
	modifyTime[4] = b[0x88] & 0x3
	copy(createTime[0:4], b[0x90:0x94])
	createTime[4] = b[0x94] & 0x3

	accessTimeSeconds := binary.LittleEndian.Uint64(accessTime)
	changeTimeSeconds := binary.LittleEndian.Uint64(changeTime)
	modifyTimeSeconds := binary.LittleEndian.Uint64(modifyTime)
	createTimeSeconds := binary.LittleEndian.Uint64(createTime)

	// now get the nanoseconds by using the upper 30 bites
	accessTimeNanoseconds := binary.LittleEndian.Uint32(b[0x8c:0x90]) >> 2
	changeTimeNanoseconds := binary.LittleEndian.Uint32(b[0x84:0x88]) >> 2
	modifyTimeNanoseconds := binary.LittleEndian.Uint32(b[0x88:0x8c]) >> 2
	createTimeNanoseconds := binary.LittleEndian.Uint32(b[0x94:0x98]) >> 2

	flagsNum := binary.LittleEndian.Uint32(b[0x20:0x24])

	flags := parseInodeFlags(flagsNum)

	blocksLow := binary.LittleEndian.Uint32(b[0x1c:0x20])
	blocksHigh := binary.LittleEndian.Uint16(b[0x74:0x76])
	var (
		blocks           uint64
		filesystemBlocks bool
	)

	hugeFile := sb.features.hugeFile
	switch {
	case !hugeFile:
		// just 512-byte blocks
		blocks = uint64(blocksLow)
		filesystemBlocks = false
	case hugeFile && !flags.hugeFile:
		// larger number of 512-byte blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = false
	default:
		// larger number of filesystem blocks
		blocks = uint64(blocksHigh)<<32 + uint64(blocksLow)
		filesystemBlocks = true
	}

	// last but not least, parse the extentTree, and convert it into an array of blocks
	extentInfo := make([]byte, 60, 60)
	copy(extentInfo, b[0x28:0x64])
	allExtents, err := parseExtentTree(b[0x28:0x64], 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error parsing extent tree: %v", err)
	}

	i := inode{
		number:                      number,
		permissionsGroup:            parseGroupPermissions(mode),
		permissionsOwner:            parseOwnerPermissions(mode),
		permissionsOther:            parseOtherPermissions(mode),
		fileType:                    parseFileType(mode),
		owner:                       binary.LittleEndian.Uint32(owner),
		group:                       binary.LittleEndian.Uint32(group),
		size:                        binary.LittleEndian.Uint64(size),
		hardLinks:                   binary.LittleEndian.Uint16(b[0x1a:0x1c]),
		blocks:                      blocks,
		filesystemBlocks:            filesystemBlocks,
		flags:                       flags,
		nfsFileVersion:              binary.LittleEndian.Uint32(b[0x64:0x68]),
		version:                     binary.LittleEndian.Uint64(version),
		inodeSize:                   binary.LittleEndian.Uint16(b[0x80:0x82]) + 128,
		deletionTime:                binary.LittleEndian.Uint32(b[0x14:0x18]),
		accessTimeSeconds:           accessTimeSeconds,
		changeTimeSeconds:           changeTimeSeconds,
		creationTimeSeconds:         createTimeSeconds,
		modificationTimeSeconds:     modifyTimeSeconds,
		accessTimeNanoseconds:       accessTimeNanoseconds,
		changeTimeNanoseconds:       changeTimeNanoseconds,
		creationTimeNanoseconds:     createTimeNanoseconds,
		modificationTimeNanoseconds: modifyTimeNanoseconds,
		extendedAttributeBlock:      binary.LittleEndian.Uint64(extendedAttributeBlock),
		project:                     binary.LittleEndian.Uint64(b[0x9c:0x100]),
		extents:                     allExtents,
	}

	return &i, nil
}

// toBytes returns an inode ready to be written to disk
func (i *inode) toBytes(sb *superblock) ([]byte, error) {
	iSize := sb.inodeSize

	b := make([]byte, iSize, iSize)

	mode := make([]byte, 2, 2)
	owner := make([]byte, 4, 4)
	fileSize := make([]byte, 8, 8)
	group := make([]byte, 4, 4)
	accessTime := make([]byte, 8, 8)
	changeTime := make([]byte, 8, 8)
	modifyTime := make([]byte, 8, 8)
	createTime := make([]byte, 8, 8)
	version := make([]byte, 8, 8)
	extendedAttributeBlock := make([]byte, 8, 8)

	binary.LittleEndian.PutUint16(mode, i.permissionsGroup.toGroupInt()|i.permissionsOther.toOtherInt()|i.permissionsOwner.toOwnerInt()|uint16(i.fileType))
	binary.LittleEndian.PutUint32(owner, i.owner)
	binary.LittleEndian.PutUint32(group, i.group)
	binary.LittleEndian.PutUint64(fileSize, i.size)
	binary.LittleEndian.PutUint64(version, i.version)
	binary.LittleEndian.PutUint64(extendedAttributeBlock, i.extendedAttributeBlock)

	binary.LittleEndian.PutUint64(accessTime, i.accessTimeSeconds)
	binary.LittleEndian.PutUint32(accessTime[4:8], (i.accessTimeNanoseconds<<2)&accessTime[4])
	binary.LittleEndian.PutUint64(createTime, i.creationTimeSeconds)
	binary.LittleEndian.PutUint32(createTime[4:8], (i.createTimeNanoseconds<<2)&createTime[4])
	binary.LittleEndian.PutUint64(changeTime, i.changeTimeSeconds)
	binary.LittleEndian.PutUint32(changeTime[4:8], (i.changeTimeNanoseconds<<2)&changeTime[4])
	binary.LittleEndian.PutUint64(modifyTime, i.modifyTimeSeconds)
	binary.LittleEndian.PutUint32(modifyTime[4:8], (i.modifyTimeNanoseconds<<2)&modifyTime[4])

	blocks := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(blocks, i.blocks)

	copy(b[0x0:0x2], mode)
	copy(b[0x2:0x4], owner[0:2])
	copy(b[0x4:0x8], fileSize[0:4])
	copy(b[0x8:0xc], accessTime[0:4])
	copy(b[0xc:0x10], changeTime[0:4])
	copy(b[0x10:0x14], modifyTime[0:4])

	binary.LittleEndian.PutUint32(b[0x14:0x18], i.deletionTime)
	copy(b[0x18:0x1a], group[0:2])
	binary.LittleEndian.PutUint16(b[0x1a:0x1c], i.hardLinks)
	copy(b[0x1c:0x20], blocks[0:4])
	binary.LittleEndian.PutUint32(b[0x20:0x24], i.flags.toInt())
	copy(b[0x24:0x28], version[0:4])
	copy(b[0x28:0x64], i.extents.toBytes())
	binary.LittleEndian.PutUint32(b[0x64:0x68], i.nfsFileVersion)
	copy(b[0x68:0x6c], extendedAttributeBlock[0:4])
	copy(b[0x6c:0x70], fileSize[4:8])
	// b[0x70:0x74] is obsolete
	copy(b[0x74:0x76], blocks[4:8])
	copy(b[0x76:0x78], extendedAttributeBlock[4:6])
	copy(b[0x78:0x7a], owner[2:4])
	copy(b[0x7a:0x7c], group[2:4])
	// b[0x7c:0x7e] is for checkeum
	// b[0x7e:0x80] is unused
	binary.LittleEndian.PutUint16(b[0x80:0x82], i.inodeSize-128)
	// b[0x82:0x84] is for checkeum
	copy(b[0x84:0x88], changeTime[4:8])
	copy(b[0x88:0x8c], modifyTime[4:8])
	copy(b[0x8c:0x90], accessTime[4:8])
	copy(b[0x90:0x94], createTime[0:4])
	copy(b[0x94:0x98], createTime[4:8])

	actualChecksum := inodeChecksum(b, superblockUuid, i.number)
	checksum := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(checksum, actualChecksum)
	copy(b[0x7c:0x7e], checksum[0:2])
	copy(b[0x82:0x84], checksum[2:4])

	return b, nil
}

func parseOwnerPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOwnerExecute == filePermissionsOwnerExecute,
		write:   mode&filePermissionsOwnerWrite == filePermissionsOwnerWrite,
		read:    mode&filePermissionsOwnerRead == filePermissionsOwnerRead,
	}
}
func parseGroupPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsGroupExecute == filePermissionsGroupExecute,
		write:   mode&filePermissionsGroupWrite == filePermissionsGroupWrite,
		read:    mode&filePermissionsGroupRead == filePermissionsGroupRead,
	}
}
func parseOtherPermissions(mode uint16) filePermissions {
	return filePermissions{
		execute: mode&filePermissionsOtherExecute == filePermissionsOtherExecute,
		write:   mode&filePermissionsOtherWrite == filePermissionsOtherWrite,
		read:    mode&filePermissionsOtherRead == filePermissionsOtherRead,
	}
}
func (fp *filePermissions) toOwnerInt() uint16 {
	return (fp.execute & filePermissionsOwnerExecute) | (fp.write & filePermissionsOwnerWrite) | (fp.read & filePermissionsOwnerRead)
}
func (fp *filePermissions) toOtherInt() uint16 {
	return (fp.execute & filePermissionsOtherExecute) | (fp.write & filePermissionsOtherWrite) | (fp.read & filePermissionsthnerRead)
}
func (fp *filePermissions) toGroupInt() uint16 {
	return (fp.execute & filePermissionsGroupExecute) | (fp.write & filePermissionsGroupWrite) | (fp.read & filePermissionsGroupRead)
}
func parseFileType(mode uint16) fileType {
	var f fileType
	switch {
	case mode&fileTypeFifo == fileTypeFifo:
		f = fileTypeFifo
	case mode&fileTypeBlockDevice == fileTypeBlockDevice:
		f = fileTypeBlockDevice
	case mode&fileTypeCha == fileTypeCharacterDevice:
		f = fileTypeCharacterDevice
	case mode&fileTypeDirectory == fileTypeDirectory:
		f = fileTypeDirectory
	case mode&fileTypeRegularFile == fileTypeRegularFile:
		f = fileTypeRegularFile
	case mode&fileTypeSocket == fileTypeSocket:
		f = fileTypeSocket
	case mode&fileTypeSymbolicLink == fileTypeSymbolicLink:
		f = fileTypeSymbolicLink
	}
	return f
}

func parseInodeFlags(flags uint32) inodeFlags {
	return inodeFlags{
		secureDeletion:          flags&inodeFlagSecureDeletion == inodeFlagSecureDeletion,
		preserveForUndeletion:   flags&inodeFlagPreserveForUndeletion == inodeFlagPreserveForUndeletion,
		compressed:              flags&inodeFlagCompressed == inodeFlagCompressed,
		synchronous:             flags&inodeFlagSynchronous == inodeFlagSynchronous,
		immutable:               flags&inodeFlagImmutable == inodeFlagImmutable,
		appendOnly:              flags&inodeFlagAppendOnly == inodeFlagAppendOnly,
		noDump:                  flags&inodeFlagNoDump == inodeFlagNoDump,
		noAccessTimeUpdate:      flags&inodeFlagNoAccessTimeUpdate == inodeFlagNoAccessTimeUpdate,
		dirtyCompressed:         flags&inodeFlagDirtyCompressed == inodeFlagDirtyCompressed,
		compressedClusters:      flags&inodeFlagCompressedClusters == inodeFlagCompressedClusters,
		noCompress:              flags&inodeFlagNoCompress == inodeFlagNoCompress,
		encryptedInode:          flags&inodeFlagEncryptedInode == inodeFlagEncryptedInode,
		hashedDirectoryIndexes:  flags&inodeFlagHashedDirectoryIndexes == inodeFlagHashedDirectoryIndexes,
		AFSMagicDirectory:       flags&inodeFlagAFSMagicDirectory == inodeFlagAFSMagicDirectory,
		alwaysJournal:           flags&inodeFlagAlwaysJournal == inodeFlagAlwaysJournal,
		noMergeTail:             flags&inodeFlagNoMergeTail == inodeFlagNoMergeTail,
		syncDirectoryData:       flags&inodeFlagSyncDirectoryData == inodeFlagSyncDirectoryData,
		topDirectory:            flags&inodeFlagTopDirectory == inodeFlagTopDirectory,
		hugeFile:                flags&inodeFlagHugeFile == inodeFlagHugeFile,
		usesExtents:             flags&inodeFlagUsesExtents == inodeFlagUsesExtents,
		extendedAttributes:      flags&inodeFlagExtendedAttributes == inodeFlagExtendedAttributes,
		blocksPastEOF:           flags&inodeFlagBlocksPastEOF == inodeFlagBlocksPastEOF,
		snapshot:                flags&inodeFlagSnapshot == inodeFlagSnapshot,
		deletingSnapshot:        flags&inodeFlagDeletingSnapshot == inodeFlagDeletingSnapshot,
		completedSnapshotShrink: flags&inodeFlagCompletedSnapshotShrink == inodeFlagCompletedSnapshotShrink,
		inlineData:              flags&inodeFlagInlineData == inodeFlagInlineData,
		inheritProject:          flags&inodeFlagInheritProject == inodeFlagInheritProject,
	}
}

func (i *inodeFlags) toInt() uint32 {
	var flags uint32

	if f.secureDeletion {
		flags = flags | inodeFlagSecureDeletion
	}
	if f.preserveForUndeletion {
		flags = flags | inodeFlagPreserveForUndeletion
	}
	if f.compressed {
		flags = flags | inodeFlagCompressed
	}
	if f.synchronous {
		flags = flags | inodeFlagSynchronous
	}
	if f.immutable {
		flags = flags | inodeFlagImmutable
	}
	if f.appendOnly {
		flags = flags | inodeFlagAppendOnly
	}
	if f.noDump {
		flags = flags | inodeFlagNoDump
	}
	if f.noAccessTimeUpdate {
		flags = flags | inodeFlagNoAccessTimeUpdate
	}
	if f.dirtyCompressed {
		flags = flags | inodeFlagDirtyCompressed
	}
	if f.compressedClusters {
		flags = flags | inodeFlagCompressedClusters
	}
	if f.noCompress {
		flags = flags | inodeFlagNoCompress
	}
	if f.encryptedInode {
		flags = flags | inodeFlagEncryptedInode
	}
	if f.hashedDirectoryIndexes {
		flags = flags | inodeFlagHashedDirectoryIndexes
	}
	if f.AFSMagicDirectory {
		flags = flags | inodeFlagAFSMagicDirectory
	}
	if f.alwaysJournal {
		flags = flags | inodeFlagAlwaysJournal
	}
	if f.noMergeTail {
		flags = flags | inodeFlagNoMergeTail
	}
	if f.syncDirectoryData {
		flags = flags | inodeFlagSyncDirectoryData
	}
	if f.topDirectory {
		flags = flags | inodeFlagTopDirectory
	}
	if f.hugeFile {
		flags = flags | inodeFlagHugeFile
	}
	if f.usesExtents {
		flags = flags | inodeFlagUsesExtents
	}
	if f.extendedAttributes {
		flags = flags | inodeFlagExtendedAttributes
	}
	if f.blocksPastEOF {
		flags = flags | inodeFlagBlocksPastEOF
	}
	if f.snapshot {
		flags = flags | inodeFlagSnapshot
	}
	if f.deletingSnapshot {
		flags = flags | inodeFlagDeletingSnapshot
	}
	if f.completedSnapshotShrink {
		flags = flags | inodeFlagCompletedSnapshotShrink
	}
	if f.inlineData {
		flags = flags | inodeFlagInlineData
	}
	if f.inheritProject {
		flags = flags | inodeFlagInheritProject
	}

	return flags
}

// parseExtentTree takes bytes, parses them to find the actual extents or the next blocks down
//  and then calls recursively to get the actual extents
func parseExtentTree(b []byte, fileBlock uint32, dataBlock uint64) (*extentTree, error) {
	// must have at least header and one entry
	minLength := extentTreeHeaderLength + extentTreeEntryLength
	if len(b) < minLength {
		return nil, fmt.Errorf("cannot parse extent tree from %d bytes, minimum required %d", len(b), minLength)
	}
	// check magic signature
	if binary.LittleEndian.Uint16(b[0:2]) != extentHeaderSignature {
		return nil, fmt.Errorf("Invalid extent tree signature: %x", b[0x0:0x2])
	}
	e := extentTree{
		entries:   binary.LittleEndian.Uint16(b[0x2:0x4]),
		max:       binary.LittleEndian.Uint16(b[0x4:0x6]),
		depth:     binary.LittleEndian.Uint16(b[0x6:0x8]),
		fileBlock: fileBlock,
		dataBlock: dataBlock,
	}
	// b[0x8:0xc] is used for the generation by Lustre but not standard ext4, so we ignore

	// we have parsed the header, now read either the leaf entries or the intermediate nodes
	switch e.depth {
	case 0:
		// read the leaves
		e.extents = extents{
			extents: make([]extent, 0, 4),
		}
		for i := 0; i < e.entries; i++ {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8, 8)
			copy(diskBlock[0:4], b[start+8:start+12])
			copy(diskBlock[4:6], b[start+6:start+8])
			e.extents.extents = append(e.extents.extents, extent{
				fileBlock:     binary.LittleEndian.Uint32(b[start : start+4]),
				count:         binary.LittleEndian.Uint16(b[start+4 : start+6]),
				startingBlock: binary.LittleEndian.Uint64(diskBlock),
			})
		}
	default:
		// read the intermediate nodes, and then go down a level to process
		e.children = make([]*extentTree, 0, 4)
		for i := 0; i < e.entries; i++ {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8, 8)
			copy(diskBlock[0:4], b[start+4:start+8])
			copy(diskBlock[4:6], b[start+8:start+10])
			// *** read the block information for that block from the disk
			child, err := parseExtentTree(block, binary.LittleEndian.Uint32(b[start:start+4]), binary.LittleEndian.Uint64(diskBlock))
			if err != nil {
				return nil, fmt.Errorf("Unable to parse extent tree child: %v", err)
			}
			e.children = append(e.children, child)
		}
	}

	return &e
}

// extendExtentTree extend extent tree with a slice of new extents
// if the existing tree is nil, create a new one
func extendExtentTree(e *extents, tree *extentTree, blockSize uint64) (*extentTree, error) {
	// our logic:
	// 1- create groups of extents, where each group fits into a single block with the header.
	//    Each group is saved in an extentTree{} struct with the elements in extentTree.extnets
	//    we now have []extentTree
	// 2- create groups of extentTree, where each group fits into a single block with the header.
	//    Each group is saved in an extentTree() struct with the elements in extentTree.children
	//    we now have []extentTree
	// 3- repeat 2 with its output until one of the following happens:
	//      a- we have a group output of 2 whose number of children <= 4, and put that in the inode
	//      b- we have repeated 2 more than 4 times (i.e. depth >= 5), in which case the file is too large

	maxEntriesPerBlock := (blockSize - extentTreeHeaderLength) / extentTreeEntryLength
	leafBlocksRequired := entries / maxEntriesPerBlock
	maxLeafNodes := extentInodeMaxEntries * math.Pow(maxEntriesPerBlock, extentTreeMaxDepth)

	// exts is the new extents to add
	exts := e.extents
	entries := len(exts)

	if tree == nil {
		tree = &extentTree{
			depth:     0,
			max:       extentInodeMaxEntries,
			fileBlock: 0,
			entries:   0,
		}
	}

	switch {
	case leafBlocksRequired > maxLeafNodes:
		// too large for ext4
		return nil, fmt.Errorf("%d extents requires %d leaf nodes, greater than the maximum of %d", entries, leafBlocksRequired, maxLeafNodes)
	case tree.depth == 0 && tree.entries+entries <= extentInodeMaxEntries:
		// existing flat tree (depth 0) with room for new extents
		tree.extents = append(tree.extents, exts)
		tree.entries += entries
	case tree.depth == 0 && tree.entries+entries > extentInodeMaxEntries:
		// existing flat tree (depth 0) with insufficient room for new extents
		// so just add ours to that one and make a new tree
		exts = append(tree.extents, exts)
		tree = buildExtentTree(exts, maxEntriesPerBlock)
	case tree.depth > 0:
		// existing deep tree - just extend it
		// take the last intermediate entry
		var lastEntry *extentTree
		for lastEntry = tree; lastEntry.depth == 0; lastEntry = lastEntry.children[lastEntries.children-1] {
		}
		// we now have the 0 depth node, so add the extents to the end
		assign := entries
		availableSlots := maxEntriesPerBlock - lastEntry.entries
		if availableSlots < assign {
			assign = availableSlots
		}
		lastEntry.extents.extents = append(lastEntry.extents.extents, exts[:assign])
		// do we have any unallocated? If so, walk up the tree to find the next one
		if entries-assign > 0 {

		}
	}

	return &tree, nil
}

func buildExtentTree(exts []*extents, maxEntriesPerBlock uint64) (*extentTree, error) {
	// new tree
	// do not forget to reserve the header
	// we now know how many leaf blocks we need, now calculate how many branch blocks
	// each leafBlock takes one entry in a branch block
	entries := len(exts)

	// 1- create groups of extents, where each group fits into a single block with the header.
	leafs := make([]*extentTree, 0, maxEntriesPerBlock)
	for i := 0; i < entries; {
		end := i + maxEntriesPerBlock
		if end > entries {
			end = entries
		}
		leafs = append(leafs, &extentTree{
			depth:       0,
			entries:     end - i,
			max:         maxEntriesPerBlock,
			blockNumber: -1, // we do not know yet what block will store these
			fileBlock:   exts[i].fileBlock,
			extents:     exts[i:end],
		})
		i = end
	}

	// 2- create groups of extentTree, where each group fits into a single block with the header.
	// 3- repeat 2 with its output, until the output of a run has <= 4 (extentInodeMaxEntries) children in the group
	root := leafs
	var depth int
	for depth = 1; len(root) < extentInodeMaxEntries; depth++ {
		nodes := make([]*extentTree, 0, maxEntriesPerBlock)
		for i := 0; i < len(root); i++ {
			end := i + maxEntriesPerBlock
			if end > len(root) {
				end = len(root)
			}
			nodes = append(nodes, &extentTree{
				depth:       depth,
				entries:     end - i,
				max:         maxEntriesPerBlock,
				blockNumber: -1, // we do not know yet what block will store these
				fileBlock:   nodes[i].fileBlock,
				children:    nodes[i:end],
			})
			i = end
		}
		root = nodes
	}
	// now just make the root node with up to extentInodeMaxEntries (4) entries
	tree = &extentTree{
		depth:       depth,
		entries:     len(root),
		max:         extentInodeMaxEntries,
		blockNumber: -1, // we do not know yet what block will store these
		fileBlock:   nodes[i].fileBlock,
		children:    nodes[i:end],
	}
	return tree
}

// extentTreeToBytes takes an extent tree and returns just the 60 bytes that go in the inode
// anything deeper in the extent tree should have been written to disk during allocation time
func (e *extentTree) toBytes() []byte {
	b := make([]byte, 60, 60)

	binary.LittleEndian.PutUint16(b[0x0:0x2], extentHeaderSignature)
	binary.LittleEndian.PutUint16(b[0x2:0x4], e.entries)
	binary.LittleEndian.PutUint16(b[0x4:0x6], e.max)
	binary.LittleEndian.PutUint16(b[0x6:0x8], e.depth)

	switch e.depth {
	case 0:
		for i, ext := range e.extents.extents {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(diskBlock, ext.startingBlock)
			copy(b[start+8:start+12], diskBlock[0:4])
			copy(b[start+6:start+8], diskBlock[4:6])
			binary.LittleEndian.PutUint32(b[start:start+4], ext.fileBlock)
			binary.LittleEndian.PutUint16(b[start+4:start+6], ext.count)
		}
	default:
		for i, child := range e.children {
			start := i*extentTreeEntryLength + extentTreeHeaderLength
			diskBlock := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(diskBlock, child.blockNumber)
			copy(b[start+4:start+8], diskBlock[0:4])
			copy(b[start+8:start+10], diskBlock[4:6])
			binary.LittleEndian.PutUint32(b[start:start+4], child.fileBlock)
		}
	}

	return b
}

// getExtents - return a sorted extents structure from a tree
func (e *extentTree) getExtents() *extents {
	// simple logic - walk the tree to read all of the extents into a single slice, and then sort them
	allextents := make([]*extent, 10)
	if e.extents != nil {
		allextents = append(allextents, e.extents.extents)
	}
	if e.children != nil {
		for _, child := range e.children {
			allextents = append(allextents, child.getExtents())
		}
	}
	// now just sort them all
	sort.Slice(allextents, func(i, j int) bool {
		return allextents[i].fileBlock < allextents[j].fileBlock
	})
	return &extents{
		extents: allextents,
	}
}

// dataBlockCount get total number of data blocks in the extent tree
// does not include the extent tree blocks themselves
func (e *extentTree) dataBlockCount() int64 {
	// simple logic - walk the tree to read all of the extents into a single slice, and then sort them
	allextents := e.getExtents()
	count := int64(0)
	for _, ext := range allextents.extents {
		count += int64(ext.count)
	}
	return count
}

// indirectBlockCount get total number of indirect blocks in the extent tree
// does not include leaf blocks with actual extents
func (e *extentTree) indirectBlockCount() int64 {
	// simple logic - walk the tree to read all of the extents into a single slice, and then sort them
	count := 0
	if e.children != nil {
		for _, child := range e.children {
			count += child.indirectBlockCount()
		}
	}
	return count
}

// inodeChecksum calculate the checksum for an inode
// NOTE: we are assuming that the inode number is uint64, but we do not know that to be true
//    it might be uint32 or uint64, and it might be in BigEndian as opposed to LittleEndian
//    just have to start with this and see
func inodeChecksum(b, superblockUuid []byte, inodeNumber uint64) uint32 {
	var input []byte

	numberBytes := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(numberBytes, inodeNumber)
	input = append(superblockUuid, numberBytes, b...)
	crc32Table := crc32.MakeTable(crc32.Castagnoli)
	checksum := crc32.Checksum(input, crc32Table)
	return checksum
}
