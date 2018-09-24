package ext4

import (
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"github.com/deitch/diskfs/filesystem"
	"github.com/deitch/diskfs/util"
	uuid "github.com/satori/go.uuid"
	bitset "github.com/willf/bitset"
)

// SectorSize indicates what the sector size in bytes is
type SectorSize uint16

// BlockSize indicates how many sectors are in a block
type BlockSize uint8

// BlockGroupSize indicates how many blocks are in a group, standardly 8*block_size_in_bytes

const (
	// SectorSize512 is a sector size of 512 bytes, used as the logical size for all ext4 filesystems
	SectorSize512        SectorSize = 512
	minBlocksPerGroup    int64      = 256
	BootSectorSize       SectorSize = 2 * SectorSize512
	SuperblockSize       SectorSize = 2 * SectorSize512
	BlockGroupFactor     int        = 8
	maxClusterSize       int        = 128
	minClusterSize       int        = 65529
	bytesPerSlot         int        = 32
	maxCharsLongFilename int        = 13
	maxBlocksPerExtent   int        = 32768
	million              int        = 1000000
	billion              int        = 1000 * million
)

type Params struct {
	Uuid               *uuid.UUID
	SectorsPerBlock    uint8
	BlocksPerGroup     int64
	SparseSuperVersion uint8
	Checksum           bool
}

// FileSystem implememnts the FileSystem interface
type FileSystem struct {
	bootSector       []byte
	superblock       *superblock
	groupDescriptors *groupDescriptors
	dataBlockBitmap  bitmap
	inodeBitmap      bitmap
	blockGroups      int64
	size             int64
	start            int64
	file             util.File
}

// Equal compare if two filesystems are equal
func (fs *FileSystem) Equal(a *FileSystem) bool {
	localMatch := fs.file == a.file
	sbMatch := fs.superblock.equal(a.superblock)
	gdMatch := fs.groupDescriptors.equal(a.groupDescriptors)
	return localMatch && sbMatch && gdMatch
}

// Create creates an ext4 filesystem in a given file or device
//
// requires the util.File where to create the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File to create the filesystem,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to create the filesystem on the entire disk. You could have a disk of size
// 20GB, and create a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for creating filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/deitch/diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
func Create(f util.File, size int64, start int64, sectorsize int64, p Params) (*FileSystem, error) {
	// sectorsize must be <=0 or exactly SectorSize512 or error
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	// there almost are no limits on an ext4 fs - theoretically up to 1 YB
	//  but we do have to check the max and min size per the requested parameters
	//if size < minSizeGivenParameters {
	//	return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d for given parameters", minSizeGivenParameters*4)
	//}
	//if size > maxSizeGivenParameters {
	//	return nil, fmt.Errorf("requested size is bigger than maximum ext4 size %d for given parameters", maxSizeGivenParameters*4)
	//}

	// uuid
	fsuuid := p.Uuid
	if fsuuid == nil {
		fsuuid2 := uuid.NewV4()
		fsuuid = &fsuuid2
	}

	// blocksize
	sectorsPerBlock := p.SectorsPerBlock
	userProvidedBlocksize := false
	switch {
	case sectorsPerBlock > 128 || sectorsPerBlock < 2:
		return nil, fmt.Errorf("Invalid sectors per block %d, must be between %d and %d sectors", sectorsPerBlock, 2, 128)
	case sectorsPerBlock < 1:
		sectorsPerBlock = 2
	default:
		userProvidedBlocksize = true
	}
	blocksize := int64(sectorsPerBlock) * sectorsize

	// how many whole blocks is that?
	numblocks := size / blocksize

	// recalculate if it was not user provided
	if !userProvidedBlocksize {
		sectorsPerBlock, blocksize, numblocks := recalculateBlocksize(numblocks, size)
	}

	// how many blocks in each block group (and therefore how many block groups)
	// if not provided, by default it is 8*blocksize (in bytes)
	blocksPerGroup := p.BlocksPerGroup
	switch {
	case blocksPerGroup <= 0:
		blocksPerGroup = blocksize * 8
	case blocksPerGroup < minBlocksPerGroup:
		return nil, fmt.Errorf("Invalid number of blocks per group %d, must be at least %d", blocksPerGroup, minBlocksPerGroup)
	case blocksPerGroup > 8*blocksize:
		return nil, fmt.Errorf("Invalid number of blocks per group %d, must be no larger than 8*blocksize of %d", blocksPerGroup, blocksize)
	case blocksPerGroup%8 != 0:
		return nil, fmt.Errorf("Invalid number of blocks per group %d, must be divisible by 8", blocksPerGroup)
	}

	// how many block groups do we have?
	blockGroups := numblocks / blocksPerGroup

	// which blocks have superblock and GDT?
	backupSuperblocks := map[int64]bool{}
	//  0 - primary
	//  ?? - backups
	switch p.SparseSuperVersion {
	case 2:
		// backups in first and last
		backupSuperblocks = map[int64]bool{
			0:               true,
			1:               true,
			blockGroups - 1: true,
		}
	default:
		for i := float64(0); ; i++ {
			block := int64(math.Pow(3, i))
			if block >= numblocks {
				break
			}
			backupSuperblocks[block] = true
		}
		for i := float64(0); ; i++ {
			block := int64(math.Pow(5, i))
			if block >= numblocks {
				break
			}
			backupSuperblocks[block] = true
		}
		for i := float64(0); ; i++ {
			block := int64(math.Pow(7, i))
			if block >= numblocks {
				break
			}
			backupSuperblocks[block] = true
		}
	}

	/*
		size calculations
		we have the total size of the disk from `size uint64`
		we have the sectorsize fixed at SectorSize512

		what do we need to determine or calculate?
		- block size
		- number of blocks
		- number of block groups
		- block groups for superblock and gdt backups
		- in each block group:
				- number of blocks in gdt
				- number of reserved blocks in gdt
				- number of blocks in inode table
				- number of data blocks

		config info:

		[defaults]
			base_features = sparse_super,large_file,filetype,resize_inode,dir_index,ext_attr
			default_mntopts = acl,user_xattr
			enable_periodic_fsck = 0
			blocksize = 4096
			inode_size = 256
			inode_ratio = 16384

		[fs_types]
			ext3 = {
				features = has_journal
			}
			ext4 = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,64bit,dir_nlink,extra_isize
				inode_size = 256
			}
			ext4dev = {
				features = has_journal,extent,huge_file,flex_bg,uninit_bg,inline_data,64bit,dir_nlink,extra_isize
				inode_size = 256
				options = test_fs=1
			}
			small = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 4096
			}
			floppy = {
				blocksize = 1024
				inode_size = 128
				inode_ratio = 8192
			}
			big = {
				inode_ratio = 32768
			}
			huge = {
				inode_ratio = 65536
			}
			news = {
				inode_ratio = 4096
			}
			largefile = {
				inode_ratio = 1048576
				blocksize = -1
			}
			largefile4 = {
				inode_ratio = 4194304
				blocksize = -1
			}
			hurd = {
			     blocksize = 4096
			     inode_size = 128
			}
	*/

	// are checksums enabled?
	checksumType := gdtChecksumNone
	if p.Checksum {
		checksumType = gdtChecksumMetadata
	}

	// create the superblock - MUST ADD IN OPTIONS
	sb := superblock{}
	gdt := groupDescriptors{}

	b, err := sb.toBytes()
	if err != nil {
		return nil, fmt.Errorf("Error converting Superblock to bytes: %v", err)
	}

	g, err := gdt.toBytes(checksumType, (*fsuuid).Bytes())
	if err != nil {
		return nil, fmt.Errorf("Error converting Group Descriptor Table to bytes: %v", err)
	}
	// how big should the GDT be?
	gdSize := groupDescriptorSize
	if sb.features.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}
	gdtSize := int64(gdSize) * numblocks
	// write the superblock and GDT to the various locations on disk
	for bg, _ := range backupSuperblocks {
		block := bg * blocksPerGroup
		blockStart := block * blocksize
		// allow that the first one requires an offset
		incr := int64(0)
		if block == 0 {
			incr = int64(SectorSize512) * 2
		}

		// write the superblock
		count, err := f.WriteAt(b, incr+blockStart+int64(start))
		if err != nil {
			return nil, fmt.Errorf("Error writing Superblock for block %d to disk: %v", block, err)
		}
		if count != int(SuperblockSize) {
			return nil, fmt.Errorf("Wrote %d bytes of Superblock for block %d to disk instead of expected %d", count, block, SuperblockSize)
		}

		// write the GDT
		count, err = f.WriteAt(g, incr+blockStart+int64(SuperblockSize)+int64(start))
		if err != nil {
			return nil, fmt.Errorf("Error writing GDT for block %d to disk: %v", block, err)
		}
		if count != int(gdtSize) {
			return nil, fmt.Errorf("Wrote %d bytes of GDT for block %d to disk instead of expected %d", count, block, gdtSize)
		}
	}

	// create root directory
	// there is nothing in there
	return &FileSystem{
		bootSector:       []byte{},
		superblock:       &sb,
		groupDescriptors: &gdt,
		blockGroups:      blockGroups,
		size:             size,
		start:            start,
		file:             f,
	}, nil
}

// Read reads a filesystem from a given disk.
//
// requires the util.File where to read the filesystem, size is the size of the filesystem in bytes,
// start is how far in bytes from the beginning of the util.File the filesystem is expected to begin,
// and blocksize is is the logical blocksize to use for creating the filesystem
//
// note that you are *not* required to read a filesystem on the entire disk. You could have a disk of size
// 20GB, and a small filesystem of size 50MB that begins 2GB into the disk.
// This is extremely useful for working with filesystems on disk partitions.
//
// Note, however, that it is much easier to do this using the higher-level APIs at github.com/deitch/diskfs
// which allow you to work directly with partitions, rather than having to calculate (and hopefully not make any errors)
// where a partition starts and ends.
//
// If the provided blocksize is 0, it will use the default of 512 bytes. If it is any number other than 0
// or 512, it will return an error.
func Read(file util.File, size int64, start int64, sectorsize int64) (*FileSystem, error) {
	// blocksize must be <=0 or exactly SectorSize512 or error
	if sectorsize != int64(SectorSize512) && sectorsize > 0 {
		return nil, fmt.Errorf("sectorsize for ext4 must be either 512 bytes or 0, not %d", sectorsize)
	}
	// we do not check for ext4 max size because it is theoreticallt 1YB, which is bigger than an int64! Even 1ZB is!
	if size < Ext4MinSize {
		return nil, fmt.Errorf("requested size is smaller than minimum allowed ext4 size %d", Ext4MinSize)
	}

	// load the information from the disk
	// read boot sector code
	bs := make([]byte, BootSectorSize, BootSectorSize)
	n, err := file.ReadAt(bs, start)
	if err != nil {
		return nil, fmt.Errorf("Could not read boot sector bytes from file: %v", err)
	}
	if uint16(n) < uint16(BootSectorSize) {
		return nil, fmt.Errorf("Only could read %d boot sector bytes from file", n)
	}

	// read the superblock
	// the superblock is one minimal block, i.e. 2 sectors
	superblockBytes := make([]byte, SuperblockSize, SuperblockSize)
	n, err = file.ReadAt(superblockBytes, start+int64(BootSectorSize))
	if err != nil {
		return nil, fmt.Errorf("Could not read superblock bytes from file: %v", err)
	}
	if uint16(n) < uint16(SuperblockSize) {
		return nil, fmt.Errorf("Only could read %d superblock bytes from file", n)
	}

	// convert the bytes into a superblock structure
	sb, err := superblockFromBytes(superblockBytes)
	if err != nil {
		return nil, fmt.Errorf("Could not interpret superblock data: %v", err)
	}

	// now read the GDT
	// how big should the GDT be?
	numblocks := sb.blockCount
	gdSize := groupDescriptorSize
	if sb.features.fs64Bit {
		gdSize = groupDescriptorSize64Bit
	}
	gdtSize := int64(gdSize) * int64(numblocks)

	gdtBytes := make([]byte, gdtSize, gdtSize)
	n, err = file.ReadAt(gdtBytes, start+int64(BootSectorSize)+int64(SuperblockSize))
	if err != nil {
		return nil, fmt.Errorf("Could not read Group Descriptor Table bytes from file: %v", err)
	}
	if int64(n) < gdtSize {
		return nil, fmt.Errorf("Only could read %d Group Descriptor Table bytes from file instead of %d", n, gdtSize)
	}
	fsuuid, err := uuid.FromString(sb.uuid)
	if err != nil {
		return nil, fmt.Errorf("Could not convert uuid %s to uuid bytes: %v", sb.uuid, err)
	}
	// what kind of checksum are we using?
	var checksumType gdtChecksumType
	switch {
	case sb.features.metadataChecksums:
		checksumType = gdtChecksumMetadata
	case sb.features.gdtChecksum:
		checksumType = gdtChecksumGdt
	default:
		checksumType = gdtChecksumNone
	}
	gdt, err := groupDescriptorsFromBytes(gdtBytes, sb.features.fs64Bit, fsuuid.Bytes(), checksumType)
	if err != nil {
		return nil, fmt.Errorf("Could not interpret Group Descriptor Table data: %v", err)
	}

	return &FileSystem{
		bootSector:       bs,
		superblock:       sb,
		groupDescriptors: gdt,
		blockGroups:      int64(numblocks),
		size:             size,
		start:            start,
		file:             file,
	}, nil
}

// Type returns the type code for the filesystem. Always returns filesystem.TypeExt4
func (fs *FileSystem) Type() filesystem.Type {
	return filesystem.TypeExt4
}

// Mkdir make a directory at the given path. It is equivalent to `mkdir -p`, i.e. idempotent, in that:
//
// * It will make the entire tree path if it does not exist
// * It will not return an error if the path already exists
func (fs *FileSystem) Mkdir(p string) error {
	_, _, err := fs.readDirWithMkdir(p, true)
	// we are not interesting in returning the entries
	return err
}

// ReadDir return the contents of a given directory in a given filesystem.
//
// Returns a slice of os.FileInfo with all of the entries in the directory.
//
// Will return an error if the directory does not exist or is a regular file and not a directory
func (fs *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	_, entries, err := fs.readDirWithMkdir(p, false)
	if err != nil {
		return nil, fmt.Errorf("Error reading directory %s: %v", p, err)
	}
	// once we have made it here, looping is done. We have found the final entry
	// we need to return all of the file info
	count := len(entries)
	ret := make([]os.FileInfo, count, count)
	inodes := make([]inode, len(entries), len(entries))
	for i, e := range entries {
		in, err := fs.readInode(int64(e.inode))
		if err != nil {
			return nil, fmt.Errorf("Could not read inode %d at position %d in directory: %v", e.inode, i, err)
		}
		ret[i] = FileInfo{
			modTime: time.Unix(in.modificationTimeSeconds, int64(in.modificationTimeNanoseconds)),
			name:    e.filename,
			size:    int64(in.size),
			isDir:   e.fileType&fileTypeDirectory == fileTypeDirectory,
		}
	}

	return ret, nil
}

// OpenFile returns an io.ReadWriter from which you can read the contents of a file
// or write contents to the file
//
// accepts normal os.OpenFile flags
//
// returns an error if the file does not exist
func (fs *FileSystem) OpenFile(p string, flag int) (filesystem.File, error) {
	// get the path
	dir := path.Dir(p)
	filename := path.Base(p)
	// if the dir == filename, then it is just /
	if dir == filename {
		return nil, fmt.Errorf("Cannot open directory %s as file", p)
	}
	// get the directory entries
	parentDir, entries, err := fs.readDirWithMkdir(dir, false)
	if err != nil {
		return nil, fmt.Errorf("Could not read directory entries for %s", dir)
	}
	// we now know that the directory exists, see if the file exists
	var targetEntry *directoryEntry
	for _, e := range entries {
		if e.filename != filename {
			continue
		}
		// cannot do anything with directories
		if e.fileType&fileTypeDirectory == fileTypeDirectory {
			return nil, fmt.Errorf("Cannot open directory %s as file", p)
		}
		// if we got this far, we have found the file
		targetEntry = e
	}

	// see if the file exists
	// if the file does not exist, and is not opened for os.O_CREATE, return an error
	if targetEntry == nil {
		if flag&os.O_CREATE == 0 {
			return nil, fmt.Errorf("Target file %s does not exist and was not asked to create", p)
		}
		// else create it
		targetEntry, err = fs.mkFile(parentDir, filename)
		if err != nil {
			return nil, fmt.Errorf("failed to create file %s: %v", p, err)
		}
	}
	// get the inode
	inodeNumber := targetEntry.inode
	inode, err := fs.readInode(int64(inodeNumber))
	if err != nil {
		return nil, fmt.Errorf("Could not read inode number %d: %v", inodeNumber, err)
	}
	offset := int64(0)
	if flag&os.O_APPEND == os.O_APPEND {
		offset = int64(inode.size)
	}
	return &File{
		directoryEntry: targetEntry,
		inode:          inode,
		isReadWrite:    flag&os.O_RDWR != 0,
		isAppend:       flag&os.O_APPEND != 0,
		offset:         offset,
		filesystem:     fs,
	}, nil
}

// readInode read a single inode from disk
func (fs *FileSystem) readInode(inodeNumber int64) (*inode, error) {
	sb := fs.superblock
	inodeSize := sb.inodeSize
	inodesPerGroup := sb.inodesPerGroup
	// figure out which block group the inode is on
	bg := (inodeNumber - 1) / int64(inodesPerGroup)
	// read the group descriptor to find out the location of the inode table
	gd := fs.groupDescriptors.descriptors[bg]
	inodeTableBlock := gd.inodeTableLocation
	inodeBytes := make([]byte, inodeSize)
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := inodeTableBlock * sb.blockSize
	// offsetInode is how many inodes in our inode is
	offsetInode := (inodeNumber - 1) % int64(inodesPerGroup)
	// offset is how many bytes in our inode is
	offset := offsetInode * int64(inodeSize)
	read, err := fs.file.ReadAt(inodeBytes, int64(byteStart)+offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read inode %d from offset %d of block %d from block group %d: %v", inodeNumber, offset, inodeTableBlock, bg, err)
	}
	if read != int(inodeSize) {
		return nil, fmt.Errorf("Read %d bytes for inode %d instead of inode size of %d", read, inodeNumber, inodeSize)
	}
	return inodeFromBytes(inodeBytes, sb, inodeNumber)
}

// writeInode write a single inode to disk
func (fs *FileSystem) writeInode(i *inode) error {
	sb := fs.superblock
	inodeSize := sb.inodeSize
	inodesPerGroup := sb.inodesPerGroup
	// figure out which block group the inode is on
	bg := (i.number - 1) / uint64(inodesPerGroup)
	// read the group descriptor to find out the location of the inode table
	gd := fs.groupDescriptors.descriptors[bg]
	inodeTableBlock := gd.inodeTableLocation
	inodeBytes := make([]byte, inodeSize)
	// bytesStart is beginning byte for the inodeTableBlock
	byteStart := inodeTableBlock * sb.blockSize
	// offsetInode is how many inodes in our inode is
	offsetInode := (i.number - 1) % uint64(inodesPerGroup)
	// offset is how many bytes in our inode is
	offset := int64(offsetInode) * int64(inodeSize)
	inodeBytes, err := i.toBytes(sb)
	if err != nil {
		return fmt.Errorf("Could not convert inode to bytes: %v", err)
	}
	wrote, err := fs.file.WriteAt(inodeBytes, offset)
	if err != nil {
		return fmt.Errorf("failed to write inode %d at offset %d of block %d from block group %d: %v", i.number, offset, inodeTableBlock, bg, err)
	}
	if wrote != int(inodeSize) {
		return fmt.Errorf("Wrote %d bytes for inode %d instead of inode size of %d", wrote, i.number, inodeSize)
	}
	return nil
}

// read directory entries for a given directory
func (fs *FileSystem) readDirectory(dir *Directory) ([]*directoryEntry, error) {
	// read the inode for the directory
	in, err := fs.readInode(int64(dir.directoryEntry.inode))
	if err != nil {
		return nil, fmt.Errorf("Could not read inode %d for directory: %v", dir.directoryEntry.inode, err)
	}
	// read the contents of the file across all blocks
	b, err := fs.readFileBytes(in)
	if err != nil {
		return nil, fmt.Errorf("error reading file bytes for inode %d: %v", in.number, err)
	}

	// convert into directory entries
	return parseDirEntries(b, fs)
}

// readFileBytes read all of the bytes for an individual file pointed at by a given inode
// normally not very useful, but helpful when reading a directory
func (fs *FileSystem) readFileBytes(in *inode) ([]byte, error) {
	// convert the extent tree into a sorted list of extents
	extents := in.extents.getExtents().extents
	// walk through each one, gobbling up the bytes
	b := make([]byte, fs.superblock.blockSize)
	for i, e := range extents {
		start := e.startingBlock * fs.superblock.blockSize
		count := uint64(e.count) * fs.superblock.blockSize
		b2 := make([]byte, count, count)
		read, err := fs.file.ReadAt(b2, int64(start))
		if err != nil {
			return nil, fmt.Errorf("Failed to read bytes for extent %d: %v", i, err)
		}
		if read != int(count) {
			return nil, fmt.Errorf("Read %d bytes instead of %d for extent %d", read, count, i)
		}
		b = append(b, b2...)
	}
	return b, nil
}

// mkSubdir make a subdirectory
// 1- allocate a single data block for the directory
// 2- create an inode in the inode table pointing to that data block
// 3- mark the inode in the inode bitmap
// 4- mark the data block in the data block bitmap
// 5- create a directory entry in the parent directory data blocks
func (fs *FileSystem) mkSubdir(parent *Directory, name string) (*directoryEntry, error) {
	// still to do:
	//  - write directory entry in parent
	//  - write inode to disk

	// create an inode
	inodeNumber, err := fs.allocateInode(int64(parent.inode))
	if err != nil {
		return nil, fmt.Errorf("Could not allocate inode for file %s: %v", name, err)
	}
	// get extents for the file - prefer in the same block group as the inode, if possible
	newExtents, err := fs.allocateExtents(1, nil, uint64(inodeNumber))
	if err != nil {
		return nil, fmt.Errorf("Could not allocate disk space for file %s: %v", name, err)
	}
	extentTreeParsed, err := extendExtentTree(newExtents, nil, fs.superblock.blockSize)
	if err != nil {
		return nil, fmt.Errorf("Could not convert extents into tree: %v", err)
	}
	// normally, after getting a tree from extents, you would need to then allocate all of the blocks
	//    in the extent tree - leafs and intermediate. However, because we are allocating a new directory
	//    with a single extent, we *know* it can fit in the inode itself (which has a max of 4), so no need

	// create a directory entry for the file
	de := directoryEntry{
		inode:    uint32(inodeNumber),
		filename: name,
		fileType: fileTypeDirectory,
	}
	parent.entries = append(parent.entries, &de)
	// write the parent out to disk
	bytesPerBlock := fs.superblock.blockSize
	b, err := parent.toBytes(int(bytesPerBlock))
	if err != nil {
		return nil, fmt.Errorf("Error writing parent to bytes: %v", err)
	}
	// check if parent has increased in size beyond allocated blocks
	parentInode, err := fs.readInode(int64(parent.inode))
	if err != nil {
		return nil, fmt.Errorf("Could not read inode %d of parent directory: %v", parent.inode)
	}
	// get the allocated space and the new size
	allocatedBlocks := parentInode.blocks
	allocatedBytes := allocatedBlocks * fs.superblock.blockSize
	requiredBytes := len(b)

	// if necessary, allocate another data block for the parent and update the extentTree
	if uint64(requiredBytes) > allocatedBytes {
		// allocate one new block
		newParentExtents, err := fs.allocateExtents(uint64(requiredBytes), parentInode.extents.getExtents(), uint64(parent.inode))
		// convert it back into a tree
		updatedTree, err := extendExtentTree(newParentExtents, parentInode.extents, fs.superblock.blockSize)
		if err != nil {
			return nil, fmt.Errorf("Could not convert updated extents to tree for parent directory: %v", err)
		}
		// save it on the parent inode
		parentInode.extents = updatedTree
		// increment the number of blocks in the parent
		parentInode.blocks++
		// write the inode back out
		iBytes, err := parentInode.toBytes(fs.superblock)
		if err != nil {
			return nil, fmt.Errorf("Could not convert updated parent inode back to bytes: %v", err)
		}
		err = fs.writeInode(parentInode)
		if err != nil {
			return nil, fmt.Errorf("Could not write updated parent inode back to bytes: %v", err)
		}
	}
	// write the directory entry in the parent
	// figure out which block it goes into, and possibly rebalance the directory entries hash tree

	// put entries for . and .. in the first block for the new directory
	initialEntries := []*directoryEntry{
		&directoryEntry{
			inode:    uint32(inodeNumber),
			filename: ".",
			fileType: fileTypeDirectory,
		},
		&directoryEntry{
			inode:    parent.inode,
			filename: "..",
			fileType: fileTypeDirectory,
		},
	}
	newDir := Directory{
		directoryEntry: de,
		root:           false,
		entries:        initialEntries,
	}
	dirBytes, err := newDir.toBytes(int(fs.superblock.blockSize))
	if err != nil {
		return nil, fmt.Errorf("Unable to convert new directory to bytes: %v", err)
	}
	// write the bytes out to disk
	wrote, err := fs.file.WriteAt(dirBytes, int64(newExtents.extents[0].startingBlock))
	if err != nil {
		return nil, fmt.Errorf("Unable to write new directory: %v", err)
	}
	if wrote != len(dirBytes) {
		return nil, fmt.Errorf("Wrote only %d bytes instead of expected %d for new directory", wrote, dirBytes)
	}

	// need current time
	now := time.Now()
	second := now.Unix()
	nano := uint32(now.Nanosecond())
	// write the inode for the new subdirectory out
	in := inode{
		number:                      uint64(inodeNumber),
		permissionsGroup:            parentInode.permissionsGroup,
		permissionsOwner:            parentInode.permissionsOwner,
		permissionsOther:            parentInode.permissionsOther,
		fileType:                    fileTypeDirectory,
		owner:                       parentInode.owner,
		group:                       parentInode.group,
		size:                        uint64(len(dirBytes)),
		hardLinks:                   2,
		blocks:                      newExtents.blocks(),
		flags:                       &inodeFlags{},
		nfsFileVersion:              0,
		version:                     0,
		inodeSize:                   parentInode.inodeSize,
		deletionTime:                0,
		accessTimeSeconds:           second,
		changeTimeSeconds:           second,
		creationTimeSeconds:         second,
		modificationTimeSeconds:     second,
		accessTimeNanoseconds:       nano,
		changeTimeNanoseconds:       nano,
		creationTimeNanoseconds:     nano,
		modificationTimeNanoseconds: nano,
		extendedAttributeBlock:      0,
		project:                     0,
		extents:                     extentTreeParsed,
	}
	// write the inode to disk

	// return
	return &de, nil
}

func (fs *FileSystem) writeDirectoryEntries(dir *Directory) error {
	// we need to save the entries of theparent
	b, err := dir.entriesToBytes(fs.bytesPerCluster)
	if err != nil {
		return fmt.Errorf("Could not create a valid byte stream for a FAT32 Entries: %v", err)
	}
	// now have to expand with zeros to the a multiple of cluster lengths
	// how many clusters do we need, how many do we have?
	clusterList, err := fs.getClusterList(dir.clusterLocation)
	if err != nil {
		return fmt.Errorf("Unable to get clusters for directory: %v", err)
	}
	extraClusters := len(b)/(int(fs.bootSector.biosParameterBlock.dos331BPB.dos20BPB.sectorsPerCluster)*fs.bytesPerCluster) - len(clusterList)
	if extraClusters > 0 {
		clusters, err := fs.allocateExtents(uint64(extraClusters), clusterList[len(clusterList)-1])
		if err != nil {
			return fmt.Errorf("Unable to allocate space for directory entries: %v", err)
		}
		clusterList = clusters
	}
	// now write everything out to the cluster list
	// read the data from all of the cluster entries in the list
	for i, cluster := range clusterList {
		// bytes where the cluster starts
		clusterStart := uint32(fs.start) + fs.dataStart + (cluster-2)*uint32(fs.bytesPerCluster)
		bStart := i * fs.bytesPerCluster
		written, err := fs.file.WriteAt(b[bStart:bStart+fs.bytesPerCluster], int64(clusterStart))
		if err != nil {
			return fmt.Errorf("Error writing directory entries: %v", err)
		}
		if written != fs.bytesPerCluster {
			return fmt.Errorf("Wrote %d bytes to cluster %d instead of expected %d", written, cluster, fs.bytesPerCluster)
		}
	}
	return nil
}

// make a file
func (fs *FileSystem) mkFile(parent *Directory, name string) (*directoryEntry, error) {
	// get a cluster chain for the file
	clusters, err := fs.allocateExtents(1, 0)
	if err != nil {
		return nil, fmt.Errorf("Could not allocate disk space for directory %s: %v", name, err)
	}
	// create a directory entry for the file
	return parent.createEntry(name, clusters[0], false)
}

// readDirWithMkdir - walks down a directory tree to the last entry
// if it does not exist, it may or may not make it
func (fs *FileSystem) readDirWithMkdir(p string, doMake bool) (*Directory, []*directoryEntry, error) {
	paths, err := splitPath(p)

	if err != nil {
		return nil, nil, err
	}
	// walk down the directory tree until all paths have been walked or we cannot find something
	// start with the root directory
	var entries []*directoryEntry
	currentDir := &Directory{
		directoryEntry: directoryEntry{
			clusterLocation: uint32(fs.table.rootDirCluster),
			isSubdirectory:  true,
			filesystem:      fs,
		},
	}
	entries, err = fs.readDirectory(currentDir)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to read directory %s", "/")
	}
	for i, subp := range paths {
		// do we have an entry whose name is the same as this name?
		found := false
		for _, e := range entries {
			if e.filenameLong != subp && e.filenameShort != subp && (!e.lowercaseShortname || (e.lowercaseShortname && strings.ToLower(e.filenameShort) != subp)) {
				continue
			}
			if !e.isSubdirectory {
				return nil, nil, fmt.Errorf("Cannot create directory at %s since it is a file", "/"+strings.Join(paths[0:i+1], "/"))
			}
			// the filename matches, and it is a subdirectory, so we can break after saving the cluster
			found = true
			currentDir = &Directory{
				directoryEntry: *e,
			}
			break

		}

		// if not, either make it, retrieve its cluster and entries, and loop;
		//  or error out
		if !found {
			if doMake {
				var subdirEntry *directoryEntry
				subdirEntry, err = fs.mkSubdir(currentDir, subp)
				if err != nil {
					return nil, nil, fmt.Errorf("Failed to create subdirectory %s", "/"+strings.Join(paths[0:i+1], "/"))
				}
				// write the directory entries to disk
				err = fs.writeDirectoryEntries(currentDir)
				if err != nil {
					return nil, nil, fmt.Errorf("Error writing directory entries to disk: %v", err)
				}
				// save where we are to search next
				currentDir = &Directory{
					directoryEntry: *subdirEntry,
				}
			} else {
				return nil, nil, fmt.Errorf("Path %s not found", "/"+strings.Join(paths[0:i+1], "/"))
			}
		}
		// get all of the entries in this directory
		entries, err = fs.readDirectory(currentDir)
		if err != nil {
			return nil, nil, fmt.Errorf("Failed to read directory %s", "/"+strings.Join(paths[0:i+1], "/"))
		}
	}
	// once we have made it here, looping is done; we have found the final entry
	return currentDir, entries, nil
}

// recalculate blocksize based on the existing number of blocks
//-      0 <= blocks <   3MM         : floppy - blocksize = 1024
//-    3MM <= blocks < 512MM         : small - blocksize = 1024
//- 512MM <= blocks < 4*1024*1024MM  : default - blocksize =
//- 4*1024*1024MM <= blocks < 16*1024*1024MM  : big - blocksize =
//- 16*1024*1024MM <= blocks   : huge - blocksize =
//
// the original code from e2fsprogs https://git.kernel.org/pub/scm/fs/ext2/e2fsprogs.git/tree/misc/mke2fs.c
func recalculateBlocksize(numblocks, size int64) (int, int, int) {
	var sectorsPerBlock, blocksize int
	switch {
	case 0 <= numblocks < 3*million:
		sectorsPerBlock = 2
		blocksize = 2 * SectorSize512
	case 3*million <= numblocks < 512*millionm:
		sectorsPerBlock = 2
		blocksize = 2 * SectorSize512
	case 512*milllion <= numblocks < 4*1024*1024*million:
		sectorsPerBlock = 2
		blocksize = 2 * SectorSize512
	case 4*1024*1024*milllion <= numblocks < 16*1024*1024*million:
		sectorsPerBlock = 2
		blocksize = 2 * SectorSize512
	case numblocks > 16*1024*1024*million:
		sectorsPerBlock = 2
		blocksize = 2 * SectorSize512
	}
	return sectorsPerBlock, blocksize, size / blocksize
}

// OLD FAT32 STUFF

// read directory entries for a given cluster
func (fs *FileSystem) getClusterList(firstCluster uint32) ([]uint32, error) {
	// first, get the chain of clusters
	complete := false
	cluster := firstCluster
	clusters := fs.table.clusters

	// do we even have a valid cluster?
	if _, ok := clusters[cluster]; !ok {
		return nil, fmt.Errorf("Invalid start cluster: %d", cluster)
	}

	clusterList := make([]uint32, 0, 5)
	for !complete {
		// save the current cluster
		clusterList = append(clusterList, cluster)
		// get the next cluster
		newCluster := clusters[cluster]
		// if it is EOC, we are done
		switch {
		case fs.table.isEoc(newCluster):
			complete = true
		case cluster <= 2:
			return nil, fmt.Errorf("Invalid cluster chain at %d", cluster)
		}
		cluster = newCluster
	}
	return clusterList, nil
}

// allocateInode allocate a single inode
// passed the parent, so it can know where to allocate it
// logic:
//  - parent is -1 : root inode, will allocate at 2
//  - parent is  2 : child of root, will try to spread out
//  - else         : try to collocate with parent, if possible
func (fs *FileSystem) allocateInode(parent int64) (int64, error) {
	inodeNumberInGroup := -1
	targetBG := -1
	parentBG := (parent - 1) / fs.superblock.blocksPerGroup
	switch parent {
	case -1:
		// allocate in the first block group
		inodeNumberInGroup = 2
		targetBG = 0
	case 2:
		// look for the least loaded group, starting with first
		leastCount := 0
		for i := 0; i < fs.blockGroups; i++ {
			freeBlocks := fs.groupDescriptors.descriptors[i].freeBlocks
			if freeBlocks > leastCount {
				leastCount = freeBlocks
				targetBG = i
			}
		}
	default:
		// start with the blockgroup the parent is in, and move forward until we find a group with at least 8 free blocks
		for i := 0; i < fs.blockGroups; i++ {
			bg := i + parentBG
			if bg > fs.blockGroups {
				bg = bg % fs.blockGroups
			}
			freeBlocks := fs.groupDescriptors.descriptors[bg].freeBlocks
			if freeBlocks >= 8 {
				targetBG = bg
				break
			}
		}
	}
	// load the inode bitmap
	descriptor := fs.groupDescriptors.descriptors[targetBG]
	bitmapLocation := descriptor.inodeBitmapLocation
	bitmapBytes := make([]byte, fs.superblock.blockSize, fs.superblock.blockSize)
	inodeOffset := bitmapLocation*fs.superblock.blockSize + fs.start
	read, err := fs.file.ReadAt(bitmap, inodeOffset)
	if err != nil {
		return nil, fmt.Errorf("Unable to read inode bitmap for blockgroup %d: %v", targetBG, err)
	}
	if read != fs.superblock.blockSize {
		return nil, fmt.Errorf("Read only %d bytes instead of expected %d for inode bitmap of block group %d", read, fs.superblock.blockSize, targetBG)
	}
	// create a bitset
	bs := bitset.New(fs.superblock.blockSize)

	if inodeNumberInGroup < 0 {
		err = bs.UnmarshalBinary(bitmapBytes)
		if err != nil {
			return nil, fmt.Errof("Unable to parse inode bitmap for blockgroup %d: %v", i, err)
		}
		// find the next free inode and allocate it
		inodeNumberInGroup = bs.NextClear(0)
	}
	// set it as marked
	bs.Set(inodeNumberInGroup)
	// reduce number of free inodes in that descriptor table
	descriptor.freeInodes--

	// get the inode bitmap as bytes
	inodeBitmapBytes, err := bs.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("Unable to create inode bitmap bytes for blockgroup %d: %v", targetBG, err)
	}
	// get the group descriptor as bytes
	checksumType := gdtChecksumNone
	if p.Checksum {
		checksumType = gdtChecksumMetadata
	}
	gdBytes, err := descriptor.toBytes(checksumType, fs.superblock.uuid)
	if err != nil {
		return nil, fmt.Errorf("Unable to create group descriptor bytes for blockgroup %d: %v", targetBG, err)
	}

	// write the inode bitmap bytes
	wrote, err := fs.file.WriteAt(inodeBitmapBytes, inodeOffset)
	if err != nil {
		return nil, fmt.Errorf("Unable to write inode bitmap for blockgroup %d: %v", targetBG, err)
	}
	if wrote != fs.superblock.blockSize {
		return nil, fmt.Errorf("Wrote only %d bytes instead of expected %d for inode bitmap of block group %d", wrote, fs.superblock.blockSize, targetBG)
	}

	// write the group descriptor bytes
	// gdt starts in block 1 of any redundant copies, specifically in BG 0
	gdtBlock := 1
	blockByteLocation := gdtBlock * fs.superblock.blockSize
	gdOffset := fs.start + blockByteLocation + targetBG*fs.superblock.groupDescriptorSize
	wrote, err = fs.file.WriteAt(gdBytes, gdOffset)
	if err != nil {
		return nil, fmt.Errorf("Unable to write group descriptor bytes for blockgroup %d: %v", targetBG, err)
	}
	if wrote != len(gdBytes) {
		return nil, fmt.Errorf("Wrote only %d bytes instead of expected %d for group descriptor of block group %d", wrote, len(gdBytes), targetBG)
	}

	// convert to absolute inodeNumber
	inodeNumber := inodeNumberInGroup * fs.superblock.inodesPerGroups

	return inodeNumber, nil
}

// allocateExtents allocate the data blocks in extents that are
// to be used for a file of a given size
// arguments are file size in bytes and existing extents
// if previous is nil, then we are not (re)sizing an existing file but creating a new one
// returns the extents to be used in order
func (fs *FileSystem) allocateExtents(size uint64, previous *extents, inode uint64) (*extents, error) {
	ext := make([]extent, 10)
	// 1- calculate how many blocks are needed
	required := size / fs.superblock.blockSize
	// 2- see how many blocks already are allocated
	allocated := 0
	if previous != nil {
		allocated = previous.blocks()
	}
	// 3- if needed, allocate new blocks in extents
	extraBlockCount := required - allocated
	// if we have enough, do not add anything
	if extraBlockCount <= 0 {
		return previous, nil
	}

	// if there are not enough blocks left
	if fs.superblock.freeBlocks < extraBlockCount {
		return nil, fmt.Errorf("Only %d blocks free, requires additional %d", fs.superblock.freeBlocks, extraBlockCount)
	}

	// now we need to look for as many contiguous blocks as possible
	// first calculate how many extents minimum are needed
	minExtents := extraBlockCount / maxBlocksPerExtent
	if extraBlockCount%maxBlocksPerExtent > 0 {
		minExtents++
	}
	// if all of the extents, except possibly the last, are maximum size, then we need minExtents extents
	// we loop through, trying to allocate an extent as large as our remaining blocks or maxBlocksPerExtent,
	//   whichever is smaller
	blockGroupCount := fs.blockGroups
	// keep track of which block groups were updated
	updatedBG := map[uint64]bool{}
	// instead of starting with BG 0, should start with BG where the inode for this file/dir is located
	for i := 0; i < blockGroupCount && len(allocated) < extraBlockCount; i++ {
		// keep track if we allocated anything in this blockgroup
		// 1- read the GDT for this blockgroup to find the location of the block bitmap
		//    and total free blocks
		// 2- read the block bitmap from disk
		// 3- find the maximum contiguous space available
		bitmapLocation := fs.groupDescriptors.descriptors[i].blockBitmapLocation
		bitmapBytes := make([]byte, fs.superblock.blockSize, fs.superblock.blockSize)
		read, err := fs.file.ReadAt(bitmap, bitmapLocation*fs.superblock.blockSize+fs.start)
		if err != nil {
			return nil, fmt.Errorf("Unable to read block bitmap for blockgroup %d: %v", i, err)
		}
		if read != fs.superblock.blockSize {
			return nil, fmt.Errorf("Read only %d bytes instead of expected %d for block bitmap of block group %d", read, fs.superblock.blockSize, i)
		}
		// create a bitset
		bs := bitset.New(fs.superblock.blockSize)
		err = bs.UnmarshalBinary(bitmapBytes)
		if err != nil {
			return nil, fmt.Errof("Unable to parse block bitmap for blockgroup %d: %v", i, err)
		}
		// now find our unused blocks and how many there are in a row as potential extents
		lastIndex := -1
		targetLength := extraBlockCount
		if targetLength > maxBlocksPerExtent {
			targetLength = maxBlocksPerExtent
		}
		for j, e := b.NextClear(0); e; j, e = b.NextClear(j + 1) {
			extentLength := j - lastIndex
			// do we want to track it?
			if extentLength >= targetLength {
				// create an extent of maximum size
				newExtent := extent{
					count:         targetLength,
					startingBlock: lastIndex,
				}
				// save the extent to our newly allocated extents list
				ext = append(ext, newExtent)
				// mark them as taken
				for k := 0; k < targetLength; k++ {
					bs.Set(k + lastIndex)
				}
				// reduce number of free blocks in this cluster
				fs.groupDescriptors.descriptors[i].freeBlocks -= targetLength
				updatedBG[i] = true
				// keep track that we allocated them
				allocated += targetLength
				// what if there is more leftover?
				// easily handle by setting j to the last previous element that was taken
				lastIndex += targetLength
				j = lastIndex
				// new target length
				targetLength = extraBlockCount - allocated
				if targetLength > maxBlocksPerExtent {
					targetLength = maxBlocksPerExtent
				}
			}
		}
	}
	// have we allocated everything we need to?
	if allocated < extraBlockCount {
		// we have not, so we need to cycle through looking for smaller extents - we could not use the entire size
	}

	// need to update the total blocks used/free in superblock
	fs.superblock.freeBlocks -= allocated
	// update the blockBitmapChecksum for any updated block groups in GDT
	// write updated superblock and GDT to disk
	// write backup copies

	return extents, nil
}
