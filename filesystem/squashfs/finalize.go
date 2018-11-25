package squashfs

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

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

// finalizeFileInfo is a file info useful for finalization
// fulfills os.FileInfo
//   Name() string       // base name of the file
//   Size() int64        // length in bytes for regular files; system-dependent for others
//   Mode() FileMode     // file mode bits
//   ModTime() time.Time // modification time
//   IsDir() bool        // abbreviation for Mode().IsDir()
//   Sys() interface{}   // underlying data source (can return nil)
type finalizeFileInfo struct {
	path           string
	target         string
	location       uint32
	recordSize     uint8
	depth          int
	name           string
	size           int64
	mode           os.FileMode
	modTime        time.Time
	isDir          bool
	isRoot         bool
	bytes          [][]byte
	parent         *finalizeFileInfo
	children       []*finalizeFileInfo
	content        []byte
	dataLocation   int64
	fileType       fileType
	inode          inode
	xattrs         map[string]string
	xAttrIndex     uint32
	links          uint32
	blocks         []*blockData
	startBlock     uint64
	fragmentBlock  uint32
	fragmentOffset uint32
	uid            uint32
	gid            uint32
}

func (fi *finalizeFileInfo) Name() string {
	return fi.name
}
func (fi *finalizeFileInfo) Size() int64 {
	return fi.size
}
func (fi *finalizeFileInfo) Mode() os.FileMode {
	return fi.mode
}
func (fi *finalizeFileInfo) ModTime() time.Time {
	return fi.modTime
}
func (fi *finalizeFileInfo) IsDir() bool {
	return fi.isDir
}
func (fi *finalizeFileInfo) Sys() interface{} {
	return nil
}

// add depth to all children
func (fi *finalizeFileInfo) addProperties(depth int) {
	fi.depth = depth
	for _, e := range fi.children {
		e.parent = fi
		e.addProperties(depth + 1)
	}
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

	// build up a table of xattrs we can store later
	xattrs := []map[string]string{}

	// 3- build out file tree
	fileList, dirList, err := walkTree(fs.Workspace())
	if err != nil {
		return fmt.Errorf("Error walking tree: %v", err)
	}

	// starting point
	root := dirList["."]
	root.addProperties(1)

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

	//
	// save file xattrs
	//
	for _, e := range fileList {
		if len(e.xattrs) > 0 {
			xattrs = append(xattrs, e.xattrs)
			e.xAttrIndex = uint32(len(e.xattrs) - 1)
		}
	}

	//
	// build inodes
	//
	inodeCount, idtable, err := populateInodes(fileList, options)
	if err != nil {
		return fmt.Errorf("Error populating inodes: %v", err)
	}
	sb.inodes = inodeCount

	//
	// create directory data
	//
	// this has to be stored, since inodes come before directories on disk,
	//   but need directory positions to write inodes
	//   if memory space becomes an issue, we will store it in a disk cache
	directories, err := createDirectoryData(fileList, fs.workspace)

	// populate directory inodes
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

func walkTree(workspace string) ([]*finalizeFileInfo, map[string]*finalizeFileInfo, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("Could not get pwd: %v", err)
	}
	// make everything relative to the workspace
	os.Chdir(workspace)
	dirList := make(map[string]*finalizeFileInfo)
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
		parentDirInfo := dirList[parentDir]

		if fi.IsDir() {
			entry.children = make([]*finalizeFileInfo, 0, 20)
			dirList[fp] = entry
		} else {
			// calculate blocks
			entry.size = fi.Size()
		}
		if !isRoot {
			parentDirInfo.children = append(parentDirInfo.children, entry)
			dirList[parentDir] = parentDirInfo
		}
		fileList = append(fileList, entry)
		return nil
	})
	// reset the workspace
	os.Chdir(cwd)
	return fileList, dirList, nil
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
	// save the information we need
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

func populateInodes(fileList []*finalizeFileInfo, options FinalizeOptions) (uint32, map[uint32]uint16, error) {
	// build up a table of uids/gids we can store later
	idtable := map[uint32]uint16{}
	// build our inodes for our files - must include all file types
	var inodeCount uint32
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
				return 0, idtable, fmt.Errorf("Unable to read target for symlink at %s: %v", e.path, err)
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
		case fileBlock:
			major, minor, err := getDeviceNumbers(e.path)
			if err != nil {
				return 0, idtable, fmt.Errorf("Unable to read major/minor device numbers for block device at %s: %v", e.path, err)
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
				return 0, idtable, fmt.Errorf("Unable to read major/minor device numbers for char device at %s: %v", e.path, err)
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
		inodeCount++
		e.inode = &inodeImpl{
			header: &inodeHeader{
				inodeType: inodeT,
				modTime:   e.ModTime(),
				mode:      e.Mode(),
				uidIdx:    uidIdx,
				gidIdx:    gidIdx,
				index:     inodeCount,
			},
			body: in,
		}
	}

	return inodeCount, idtable, nil
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
