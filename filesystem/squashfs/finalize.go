package squashfs

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/diskfs/go-diskfs/util"
	"github.com/pkg/xattr"
)

type fileType uint8

const (
	fileRegular fileType = iota
	fileDirectory
	fileSymlink
	fileBlock
	fileChar
	fileFifo
	fileSocket
)

// FinalizeOptions options to pass to finalize
type FinalizeOptions struct {
	// Compressor which compressor to use, including, where relevant, options. Defaults ot CompressorGzip
	Compression Compressor
	// NonExportable prevent making filesystem NFS exportable. Defaults to false, i.e. make it exportable
	NonExportable bool
	// NonSparse prevent detecting sparse files. Defaults to false, i.e. detect sparse files
	NonSparse bool
	// Xattrs whether or not to store extended attributes. Defaults to false
	Xattrs bool
	// NoCompressInodes whether or not to compress inodes. Defaults to false, i.e. compress inodes
	NoCompressInodes bool
	// NoCompressData whether or not to compress data blocks. Defaults to false, i.e. compress data
	NoCompressData bool
	// NoCompressFragments whether or not to compress fragments. Defaults to false, i.e. compress fragments
	NoCompressFragments bool
	// NoCompressXattrs whether or not to compress extended attrbutes. Defaults to false, i.e. compress xattrs
	NoCompressXattrs bool
	// NoFragments do not use fragments, but rather dedicated data blocks for all files. Defaults to false, i.e. use fragments
	NoFragments bool
	// NoPad do not pad filesystem so it is a multiple of 4K. Defaults to false, i.e. pad it
	NoPad bool
	// FileUID set all files to be owned by the UID provided, default is to leave as in filesystem
	FileUID *uint32
	// FileGID set all files to be owned by the GID provided, default is to leave as in filesystem
	FileGID *uint32
}

// Finalize finalize a read-only filesystem by writing it out to a read-only format
func (fs *FileSystem) Finalize(options FinalizeOptions) error {
	if fs.workspace == "" {
		return fmt.Errorf("Cannot finalize an already finalized filesystem")
	}

	/*
		There is nothing we can find about the order of files/directories, for any of:
		- inodes in inode table
		- entries in directory table
		- data in data section
		- fragments in fragment section

		to keep it simple, we will follow what mksquashfs on linux does, in the following order:
		- superblock at byte 0
		- compression options, if any, at byte 96
		- file data immediately following compression options (or superblock, if no compression options)
		- fragments immediately following file data
		- inode table
		- directory table
		- fragment table
		- export table
		- uid/gid lookup table
		- xattr table

		Note that until we actually copy and compress each section, we do not know the position of each subsequent
		section. So we have to write one, keep track of it, then the next, etc.


	*/

	f := fs.file
	blocksize := int(fs.blocksize)
	s := &superblock{
		blocksize: uint32(blocksize),
	}
	if options.Compression != nil {
		s.compression = options.Compression.flavour()
	}

	// build out file and directory tree
	fileList, err := walkTree(fs.Workspace())
	if err != nil {
		return fmt.Errorf("Error walking tree: %v", err)
	}

	// location holds where we are writing in our file
	var location int64
	location += superblockSize
	var b []byte
	b = options.Compression.optionsBytes()
	if len(b) > 0 {
		f.WriteAt(b, location)
		location += int64(len(b))
	}

	// next write the file blocks
	compressor := options.Compression
	if options.NoCompressData {
		compressor = nil
	}

	//
	// hold our superblock
	//
	sb := &superblock{}

	//
	// write file data blocks
	//
	dataWritten, err := writeDataBlocks(fileList, f, fs.workspace, blocksize, compressor, location)
	if err != nil {
		return fmt.Errorf("Error writing file data blocks: %v", err)
	}
	location += int64(dataWritten)

	//
	// write file fragments
	//
	fragmentBlocks, fragsWritten, err := writeFragmentBlocks(fileList, f, fs.workspace, blocksize, options, location)
	if err != nil {
		return fmt.Errorf("Error writing file fragment blocks: %v", err)
	}
	location += int64(fragsWritten)
	sb.fragmentCount = uint32(fragmentBlocks)

	// extract extended attributes
	xattrs := extractXattrs(fileList)

	// We have a chicken and an egg problem. On the one hand, inodes
	// are written to the disk before the directories, so we need to know
	// the size of the inode data. On the other hand, directory data must be known
	// to create inodes. Further complicating matters is that the data in the
	// directory inodes relies on having the directory data ready. Specifically,
	// it includes:
	// - index of the block in the directory table where the dir info starts. Note
	//   that this is not just the directory *table* index, but the *block* index.
	// - offset within the block in the directory table where the dir info starts.
	//   Same notes as previous entry.
	// - size of the directory table entries for this directory, all of it. Thus,
	//   you have to have converted it all to bytes to get the information.
	//
	// On the other hand, the directory entry (and header) needs to know what
	// inode number it points to in the entry.
	//
	// The only possible way to do this is to run one, then the other, then
	// modify them. Until you generate both, you just don't know.
	//
	// Something that eases it a bit is that the block index in directory inodes
	// is from the start of the directory table, rather than start of archive.
	//
	// Order of execution:
	// 1. Write the file (not directory) data and fragments to disk.
	// 2. Create inodes for the files. We cannot write them yet because we need to
	//    add the directory entries before compression.
	// 3. Convert the directories to a directory table. And no, we cannot just
	//    calculate it based on the directory size, since some directories have
	//    one header, some have multiple, so the size of each directory, even
	//    given the number of files, can change.
	// 4. Create inodes for the directories.
	// 5. Write inodes to disk.
	// 6. Update the directory entries based on the inodes.
	// 7. Write directory table to disk
	//
	// if storing the inodes and directory table entirely in memory becomes
	// burdensome, use temporary scratch disk space to cache data in flight

	//
	// Build inodes for files. They are saved onto the fileList items themselves.
	//
	// build up a table of uids/gids we can store later
	idtable := map[uint32]uint16{}
	// get the inodes in order as a slice
	inodes, err := createInodes(fileList, idtable, options)
	if err != nil {
		return fmt.Errorf("error creating file inodes: %v", err)
	}

	// convert the inodes to data, while keeping track of where each
	// one is, so we can update the directory entries
	inodeLocations := getInodeLocations(inodes)

	// create the directory table. We already have every inode and its position,
	// so we do not need to dip back into the inodes. The only changes will be
	// the block/offset references into the directory table, but those sizes do
	// no change. However, we will have to break out the headers, so this is not
	// completely finalized yet.
	directories := createDirectories(fileList[0], inodeLocations)

	// create the final version of the directory table by creating the headers
	// and entries.
	dirTable := optimizeDirectoryTable(directories)

	if err := updateInodesFromDirectories(inodes, dirTable, directoryLocations); err != nil {
		return fmt.Errorf("error updating inodes with final directory data: %v", err)
	}

	// write the inodes to the file
	inodesWritten, err := writeInodes(inodes, f, fs.workspace, blocksize, compressor, location)
	if err != nil {
		return fmt.Errorf("Error writing inode data blocks: %v", err)
	}
	location += int64(inodesWritten)
	// save how many inodes we have in the superblock
	sb.inodes = uint32(len(inodes))

	// write directory data
	dirsWritten, err := writeDirectories(fileList, f, fs.workspace, blocksize, compressor, location)
	if err != nil {
		return fmt.Errorf("Error writing directory data blocks: %v", err)
	}
	location += int64(dirsWritten)

	/*
		The indexCount is used for indexed lookups.

		The index is stored at the end of the inode (after the filename) for extended directory
		There is one entry for each block after the 0th, so if there is just one block, then there is no index
		The filenames in the directory are sorted alphabetically. Each entry gives the first filename found in
		the respective block, so if the name found is larger than yours, it is in the previous block

		b[0:4] uint32 index - number of bytes where this entry is from the beginning of this directory
		b[4:8] uint32 startBlock - number of bytes in the filesystem from the start of the directory table that this block is
		b[8:12] uint32 size - size of the name (-1)
		b[12:12+size] string name

		Here is an example of 1 entry:

		f11f 0000 0000 0000 0b00 0000 6669 6c65 6e61 6d65 5f34 3638

		b[0:4] index 0x1ff1
		b[4:8] startBlock 0x00
		b[8:12] size 0x0b (+1 for a total of 0x0c = 12)
		b[12:24] name filename_468
	*/

	// build the inode table

	// TODO:
	/*
		 FILL IN:
		 - inodes
		 - directories
		 - fragment table
		 - export table
		 - uidgid table
		 - xattr table

		ALSO:
		- we have been treating every file like it is a normal file, but need to handle all of the special cases:
				- symlink, IPC, block/char device, hardlink
		- add directory inodes to the list
		- create directory entries
		- deduplicate values in xattrs
		- utilize options to: not add xattrs; not compress things; etc.

	*/

	// finish by setting as finalized
	fs.workspace = ""
	return nil
}

func copyFileData(from, to util.File, fromOffset, toOffset, blocksize int64, c Compressor) (int, int, []*blockData, error) {
	buf := make([]byte, blocksize)
	raw, compressed := 0, 0
	blocks := make([]*blockData, 0)
	for {
		n, err := from.ReadAt(buf, fromOffset+int64(raw))
		if err != nil && err != io.EOF {
			return raw, compressed, nil, err
		}
		if n != len(buf) {
			break
		}
		raw += len(buf)

		// compress the block if needed
		isCompressed := false
		if c != nil {
			out, err := c.compress(buf)
			if err != nil {
				return 0, 0, nil, fmt.Errorf("Error compressing block: %v", err)
			}
			if len(out) < len(buf) {
				isCompressed = true
				buf = out
			}
		}
		blocks = append(blocks, &blockData{size: uint32(len(buf)), compressed: isCompressed})
		if _, err := to.WriteAt(buf[:n], toOffset+int64(compressed)); err != nil {
			return raw, compressed, blocks, err
		}
		compressed += len(buf)
	}
	return raw, compressed, blocks, nil
}

//
func finalizeFragment(buf []byte, to util.File, toOffset int64, c Compressor) (int, error) {
	// compress the block if needed
	if c != nil {
		out, err := c.compress(buf)
		if err != nil {
			return 0, fmt.Errorf("Error compressing fragment block: %v", err)
		}
		if len(out) < len(buf) {
			buf = out
		}
	}
	if _, err := to.WriteAt(buf, toOffset); err != nil {
		return 0, err
	}
	return len(buf), nil
}

// walkTree walks the tree and returns a slice of files and directories.
// We do files and directories differently, since they need to be processed
// differently on disk (file data and fragments vs directory table), and
// because the inode data is different.
// The first entry in the return always will be the root
func walkTree(workspace string) ([]*finalizeFileInfo, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get pwd: %v", err)
	}
	// make everything relative to the workspace
	os.Chdir(workspace)
	dirMap := make(map[string]*finalizeFileInfo)
	fileList := make([]*finalizeFileInfo, 0)
	var entry *finalizeFileInfo
	filepath.Walk(".", func(fp string, fi os.FileInfo, err error) error {
		isRoot := fp == "."
		name := fi.Name()
		m := fi.Mode()
		var fType fileType
		switch {
		case m&os.ModeSocket == os.ModeSocket:
			fType = fileSocket
		case m&os.ModeSymlink == os.ModeSymlink:
			fType = fileSymlink
		case m&os.ModeNamedPipe == os.ModeNamedPipe:
			fType = fileFifo
		case m&os.ModeDir == os.ModeDir:
			fType = fileDirectory
		case m&os.ModeDevice == os.ModeDevice && m&os.ModeCharDevice == os.ModeCharDevice:
			fType = fileChar
		case m&os.ModeDevice == os.ModeDevice && m&os.ModeCharDevice != os.ModeCharDevice:
			fType = fileBlock
		default:
			fType = fileRegular
		}
		xattrNames, err := xattr.List(fp)
		if err != nil {
			return fmt.Errorf("Unable to list xattrs for %s: %v", fp, err)
		}
		xattrs := map[string]string{}
		for _, name := range xattrNames {
			val, err := xattr.Get(fp, name)
			if err != nil {
				return fmt.Errorf("Unable to get xattr %s for %s: %v", name, fp, err)
			}
			xattrs[name] = string(val)
		}
		var (
			nlink uint32
			uid   uint32
			gid   uint32
		)
		if sys := fi.Sys(); sys != nil {
			if stat, ok := sys.(*syscall.Stat_t); ok {
				nlink = uint32(stat.Nlink)
				uid = stat.Uid
				gid = stat.Gid
			}
		}

		entry = &finalizeFileInfo{
			path:     fp,
			name:     name,
			isDir:    fi.IsDir(),
			isRoot:   isRoot,
			modTime:  fi.ModTime(),
			mode:     m,
			fileType: fType,
			size:     fi.Size(),
			xattrs:   xattrs,
			uid:      uid,
			gid:      gid,
			links:    nlink,
		}

		// we will have to save it as its parent
		parentDir := filepath.Dir(fp)
		parentDirInfo := dirMap[parentDir]

		if fi.IsDir() {
			entry.children = make([]*finalizeFileInfo, 0, 20)
			dirMap[fp] = entry
		} else {
			// calculate blocks
			entry.size = fi.Size()
		}
		if !isRoot {
			parentDirInfo.children = append(parentDirInfo.children, entry)
			dirMap[parentDir] = parentDirInfo
		}
		fileList = append(fileList, entry)
		return nil
	})
	// reset the workspace
	os.Chdir(cwd)

	return fileList, nil
}

func getTableIdx(m map[uint32]uint16, index uint32) uint16 {
	for k, v := range m {
		if k == index {
			return v
		}
	}
	// if we made it this far it doesn't exist, so add it
	m[index] = uint16(len(m))
	return m[index]
}

func getDeviceNumbers(path string) (uint32, uint32, error) {
	stat := syscall.Stat_t{}
	err := syscall.Stat("/dev/sda", &stat)
	if err != nil {
		return 0, 0, err
	}
	return uint32(stat.Rdev / 256), uint32(stat.Rdev % 256), nil
}

func writeFileDataBlocks(e *finalizeFileInfo, to util.File, ws string, startBlock uint64, blocksize int, compressor Compressor, location int64) (int, int, error) {
	from, err := os.Open(path.Join(ws, e.path))
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open file for reading %s: %v", e.path, err)
	}
	defer from.Close()
	raw, compressed, blocks, err := copyFileData(from, to, 0, location, int64(blocksize), compressor)
	if err != nil {
		return 0, 0, fmt.Errorf("Error copying file %s: %v", e.Name(), err)
	}
	if raw%blocksize != 0 {
		return 0, 0, fmt.Errorf("Copying file %s copied %d which is not a multiple of blocksize %d", e.Name(), raw, blocksize)
	}
	// save the information we need for usage later in inodes to find the file data
	e.dataLocation = location
	e.blocks = blocks
	e.startBlock = startBlock

	// how many blocks did we write?
	blockCount := raw / blocksize

	return blockCount, compressed, nil
}

func writeFileFragments(e *finalizeFileInfo, fragmentData []byte, f util.File, ws string, blocksize int, location int64, block uint32, compressor Compressor) ([]byte, int64, bool, error) {
	var (
		written int64
		wrote   bool
	)
	// how much is left?
	remainder := e.Size() % int64(blocksize)
	if remainder != 0 {
		// if we cross the blocksize, save and reset the fragment
		if len(fragmentData)+int(remainder) > blocksize {
			write, err := finalizeFragment(fragmentData, f, location, compressor)
			if err != nil {
				return nil, written, wrote, fmt.Errorf("Error writing fragments to disk: %v", err)
			}
			written = int64(write)
			wrote = true
			fragmentData = make([]byte, 0)
		}

		e.fragmentBlock = block
		e.fragmentOffset = uint32(len(fragmentData))
		from, err := os.Open(path.Join(ws, e.path))
		if err != nil {
			return nil, written, wrote, fmt.Errorf("failed to open file for reading %s: %v", e.path, err)
		}
		defer from.Close()
		buf := make([]byte, remainder)
		n, err := from.ReadAt(buf, e.Size()-remainder)
		if err != nil && err != io.EOF {
			return nil, written, wrote, fmt.Errorf("Error reading final %d bytes from file %s: %v", remainder, e.Name(), err)
		}
		if n != len(buf) {
			return nil, written, wrote, fmt.Errorf("Failed reading final %d bytes from file %s, only read %d", remainder, e.Name(), n)
		}
		from.Close()
		if err != nil {
			return nil, written, wrote, fmt.Errorf("Error getting fragment data for %s: %v", e.path, err)
		}
		fragmentData = append(fragmentData, buf...)
	}
	return fragmentData, written, wrote, nil
}

func writeDataBlocks(fileList []*finalizeFileInfo, f util.File, ws string, blocksize int, compressor Compressor, location int64) (int, error) {
	allBlocks := 0
	allWritten := 0
	for _, e := range fileList {
		// only copy data for normal files
		if e.fileType != fileRegular {
			continue
		}

		blocks, written, err := writeFileDataBlocks(e, f, ws, uint64(allBlocks), blocksize, compressor, location)
		if err != nil {
			return allWritten, fmt.Errorf("Error writing data for %s to file: %v", e.path, err)
		}
		allBlocks += blocks
		allWritten += written
	}
	return allWritten, nil
}

func writeFragmentBlocks(fileList []*finalizeFileInfo, f util.File, ws string, blocksize int, options FinalizeOptions, location int64) (int, int64, error) {
	compressor := options.Compression
	if options.NoCompressFragments {
		compressor = nil
	}
	fragmentData := make([]byte, 0)
	var allWritten int64
	fragmentBlock := 0
	for _, e := range fileList {
		// only copy data for normal files
		if e.fileType != fileRegular {
			continue
		}
		var (
			written int64
			wrote   bool
			err     error
		)
		fragmentData, written, wrote, err = writeFileFragments(e, fragmentData, f, ws, blocksize, location, uint32(fragmentBlock), compressor)
		if err != nil {
			return fragmentBlock, allWritten, fmt.Errorf("Error writing fragment data for %s: %v", e.path, err)
		}
		allWritten += written
		if wrote {
			fragmentBlock++
		}
	}

	//
	// write remaining fragment data
	//
	if len(fragmentData) > 0 {
		size, err := finalizeFragment(fragmentData, f, location, compressor)
		if err != nil {
			return fragmentBlock, allWritten, fmt.Errorf("Error writing fragments to disk: %v", err)
		}
		fragmentBlock++
		allWritten += int64(size)
	}
	return fragmentBlock, allWritten, nil
}

func createInodes(fileList []*finalizeFileInfo, idtable map[uint32]uint16, options FinalizeOptions) ([]inode, error) {
	// get the inodes
	inodes := make([]inode, 0)
	var inodeIndex uint32

	// need to keep track of directory position in directory table
	// build our inodes for our files - must include all file types
	for _, e := range fileList {
		var (
			in     inodeBody
			inodeT inodeType
		)
		switch e.fileType {
		case fileRegular:
			/*
				use an extendedFile if any of the above is true:
				- startBlock (from beginning of data section) does not fit in uint32
				- fileSize does not fit in uint32
				- it is a sparse file
				- it has extended attributes
				- it has hard links
			*/
			if e.startBlock|uint32max != uint32max || e.Size()|int64(uint32max) != int64(uint32max) || len(e.xattrs) > 0 || e.links > 0 {
				// use extendedFile inode
				in = &extendedFile{
					startBlock:         e.startBlock,
					fileSize:           uint64(e.Size()),
					blockSizes:         e.blocks,
					links:              e.links,
					fragmentBlockIndex: e.fragmentBlock,
					fragmentOffset:     e.fragmentOffset,
					xAttrIndex:         e.xAttrIndex,
				}
				inodeT = inodeExtendedFile
			} else {
				// use basicFile
				in = &basicFile{
					startBlock:         uint32(e.startBlock),
					fileSize:           uint32(e.Size()),
					blockSizes:         e.blocks,
					fragmentBlockIndex: e.fragmentBlock,
					fragmentOffset:     e.fragmentOffset,
				}
				inodeT = inodeBasicFile
			}
		case fileSymlink:
			/*
				use an extendedSymlink if it has extended attributes
				- startBlock (from beginning of data section) does not fit in uint32
				- fileSize does not fit in uint32
				- it is a sparse file
				- it has extended attributes
				- it has hard links
			*/
			target, err := os.Readlink(e.path)
			if err != nil {
				return inodes, fmt.Errorf("Unable to read target for symlink at %s: %v", e.path, err)
			}
			if len(e.xattrs) > 0 {
				in = &extendedSymlink{
					links:      e.links,
					target:     target,
					xAttrIndex: e.xAttrIndex,
				}
				inodeT = inodeExtendedSymlink
			} else {
				in = &basicSymlink{
					links:  e.links,
					target: target,
				}
				inodeT = inodeBasicSymlink
			}
		case fileDirectory:
			/*
				use an extendedDirectory if any of the following is true:
				- the directory itself has extended attributes
				- the size of the directory does not fit in a single metadata block, i.e. >8K uncompressed
				- it has more than 256 entries
			*/
			if e.startBlock|uint32max != uint32max || e.Size()|int64(uint32max) != int64(uint32max) || len(e.xattrs) > 0 || e.links > 0 {
				// use extendedFile inode
				in = &extendedDirectory{
					startBlock: e.startBlock,
					fileSize:   uint64(e.Size()),
					links:      e.links,
					xAttrIndex: e.xAttrIndex,
				}
				inodeT = inodeExtendedDirectory
			} else {
				// use basicFile
				in = &basicDirectory{
					startBlock: uint32(e.startBlock),
					links:      e.links,
					fileSize:   uint32(e.Size()),
				}
				inodeT = inodeBasicDirectory
			}
		case fileBlock:
			major, minor, err := getDeviceNumbers(e.path)
			if err != nil {
				return inodes, fmt.Errorf("Unable to read major/minor device numbers for block device at %s: %v", e.path, err)
			}
			if len(e.xattrs) > 0 {
				in = &extendedBlock{
					extendedDevice{
						links:      e.links,
						major:      major,
						minor:      minor,
						xAttrIndex: e.xAttrIndex,
					},
				}
				inodeT = inodeExtendedBlock
			} else {
				in = &basicBlock{
					basicDevice{
						links: e.links,
						major: major,
						minor: minor,
					},
				}
				inodeT = inodeBasicBlock
			}
		case fileChar:
			major, minor, err := getDeviceNumbers(e.path)
			if err != nil {
				return inodes, fmt.Errorf("Unable to read major/minor device numbers for char device at %s: %v", e.path, err)
			}
			if len(e.xattrs) > 0 {
				in = &extendedChar{
					extendedDevice{
						links:      e.links,
						major:      major,
						minor:      minor,
						xAttrIndex: e.xAttrIndex,
					},
				}
				inodeT = inodeExtendedChar
			} else {
				in = &basicChar{
					basicDevice{
						links: e.links,
						major: major,
						minor: minor,
					},
				}
				inodeT = inodeBasicChar
			}
		case fileFifo:
			if len(e.xattrs) > 0 {
				in = &extendedFifo{
					extendedIPC{
						links:      e.links,
						xAttrIndex: e.xAttrIndex,
					},
				}
				inodeT = inodeExtendedFifo
			} else {
				in = &basicFifo{
					basicIPC{
						links: e.links,
					},
				}
				inodeT = inodeBasicFifo
			}
		case fileSocket:
			if len(e.xattrs) > 0 {
				in = &extendedSocket{
					extendedIPC{
						links:      e.links,
						xAttrIndex: e.xAttrIndex,
					},
				}
				inodeT = inodeExtendedSocket
			} else {
				in = &basicSocket{
					basicIPC{
						links: e.links,
					},
				}
				inodeT = inodeBasicSocket
			}
		}
		// set the uid and gid
		uid := e.uid
		gid := e.gid
		if options.FileUID != nil {
			uid = *options.FileUID
		}
		if options.FileGID != nil {
			gid = *options.FileGID
		}
		// get index to the uid and gid
		uidIdx := getTableIdx(idtable, uid)
		gidIdx := getTableIdx(idtable, gid)
		e.inode = &inodeImpl{
			header: &inodeHeader{
				inodeType: inodeT,
				modTime:   e.ModTime(),
				mode:      e.Mode(),
				uidIdx:    uidIdx,
				gidIdx:    gidIdx,
				index:     inodeIndex,
			},
			body: in,
		}
		inodes = append(inodes, e.inode)
		inodeIndex++
	}

	return inodes, nil
}

func createDirectoryData(fileList []*finalizeFileInfo, ws string) ([]byte, error) {
	directories := make([]byte, 0)
	for _, f := range fileList {
		// only process directories
		if !f.IsDir() {
			continue
		}

		// each directory entry needs to point to its children on disk
		// the reverse direction is in directory.go:parseDirectory()
		//  there it reads the raw data; we need to do the reverse - for each directory, create the raw data
		//  look there, it consists of a header and one or more entries
		//  remember that the common information is moved to the header, while unique data
		//     per directory entry is in the entry. This saves space.

	}

	return directories, nil
}

// dirMapToList convert the map of directories to a list. The list should
// be sorted with directories in any order, given that root "." is first,
// and within a directory, sorted "asciibetically".
// Typically, all children of a given directory have sequential inode numbers.
// Our sorting should do this already. However, we could improve the algorithm.
func dirMapToList(dirMap map[string]*finalizeFileInfo) []*finalizeFileInfo {
	// add the depth to each one
	root := dirMap["."]
	root.addProperties(1)

	list := make([]*finalizeFileInfo, 0, len(dirMap))
	for _, v := range dirMap {
		// we already did root
		if !v.isRoot {
			list = append(list, v)
		}
	}

	return list
}

func extractXattrs(list []*finalizeFileInfo) []map[string]string {
	xattrs := []map[string]string{}
	for _, e := range list {
		if len(e.xattrs) > 0 {
			xattrs = append(xattrs, e.xattrs)
			e.xAttrIndex = uint32(len(e.xattrs) - 1)
		}
	}
	return xattrs
}

// blockPosition position of something inside a data or metadata section.
// Includes the block number relative to the start, and the offset within
// the block.
type blockPosition struct {
	block  uint32
	offset uint16
}

// createDirectories take a list of finalizeFileInfo, turn it into a slice of
// []directory.
func createDirectories(e *finalizeFileInfo, inodeLocations []blockPosition) []*directory {
	dirs := make([]*directory, 0)
	entries := make([]*directoryEntryRaw, 0)
	// go through each entry, and create a directory structure for it
	// we will cycle through each directory, creating an entry for it
	// and its children. A second pass will split into headers
	for _, child := range e.children {
		index := child.inode.index()
		blockPos := inodeLocations[index]
		entry := &directoryEntryRaw{
			name:           child.Name(),
			isSubdirectory: !child.isRoot,
			startBlock:     blockPos.block,
			offset:         blockPos.offset,
			// we do not yet know the inodeNumber, which is an offset from the one in the header
			// it will be filled in later
		}
		entries = append(entries, entry)
	}
	dir := &directory{
		entries: entries,
	}
	dirs = append(dirs, dir)
	// do children in a separate loop, so that we get all of the children lined up
	for _, child := range e.children {
		if child.IsDir() {
			dirs = append(dirs, createDirectories(child, inodeLocations)...)
		}
	}
	return dirs
}

// optimizeDirectoryTable convert a slice of directoryEntryRaw into a []directory
// that can be written to disk
func optimizeDirectoryTable(directories []*directory) []*directory {
	// each directory is just a list of directoryEntryRaw; pull out the common
	// elements for each into a header.
	dirs := make([]*directory, 0)
	return dirs
}

// getInodeLocations get a map of each inode index and where it will be on disk
// i.e. the inode block, and the offset into the block
func getInodeLocations(inodes []inode) []blockPosition {
	// keeps our reference
	positions := make([]blockPosition, 0, len(inodes))
	var pos int64

	// get block position for each inode
	for _, i := range inodes {
		b := i.toBytes()
		positions = append(positions, blockPosition{
			block:  uint32(pos / metadataBlockSize),
			offset: uint16(pos % metadataBlockSize),
		})
		pos += int64(len(b))
	}

	return positions
}

// getDirectoryLocations get a map of each directory index and where it will be
// on disk i.e. the directory block, and the offset into the block
func getDirectoryLocations(directories []*directory) []blockPosition {
	// keeps our reference
	refTable := make([]blockPosition, 0, len(directories))
	pos := 0

	// get block position for each inode
	for _, d := range directories {
		// we start without knowing the inode block/number
		// in any case, this func is just here to give us sizes and therefore
		// locations inside the directory metadata blocks, not actual writable
		// bytes
		b := d.toBytes(0, 0)
		refTable = append(refTable, blockPosition{
			block:  pos / metadataBlockSize,
			offset: pos % metadataBlockSize,
		})
		pos += b
	}

	return refTable
}

// updateInodesFromDirectories update the blockPosition for each directory
// inode.
func updateInodesFromDirectories(inodes []inode, dirTable []*directory, dirLocations []blockPosition) error {
	// go through each directory, find its inode, and update it with the
	// correct block and offset
	for i, d := range dirTable {
		index := d.inodeIndex
		in := inodes[index]
		switch in.(type) {
		case basicDirectory:
			dir := in.(basicDirectory)
			dir.startBlock = dirLocations[i].block
			dir.offset = dirLocations[i].offset
		case extendedDirectory:
			dir := in.(extendedDirectory)
			dir.startBlock = dirLocations[i].block
			dir.offset = dirLocations[i].offset
		default:
			return fmt.Errorf("inode at index %d from directory at index %d was unexpected type", index, i)
		}
	}
}
