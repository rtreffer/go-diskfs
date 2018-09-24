package ext4

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	uuid "github.com/satori/go.uuid"
)

type filesystemState uint16
type errorBehaviour uint16
type osFlag uint32
type feature uint32
type hashAlgorithm byte
type mountOption uint32
type flag uint32
type encryptionAlgorithm byte

const (
	// superblockSIgnature is the signature for every superblock
	superblockSignature uint16 = 0xef53
	// optional states for the filesystem
	fsStateCleanlyUnmounted filesystemState = 0x0001
	fsStateErrors           filesystemState = 0x0002
	fsStateOrphansRecovered filesystemState = 0x0004
	// how to handle erorrs
	errorsContinue        errorBehaviour = 1
	errorsRemountReadOnly errorBehaviour = 2
	errorsPanic           errorBehaviour = 3
	// checksum type
	crc32c byte = 1
	// oses
	osLinux   osFlag = 0
	osHurd    osFlag = 1
	osMasix   osFlag = 2
	osFreeBSD osFlag = 3
	osLites   osFlag = 4
	// compatible, incompatible, and compatibleReadOnly feature flags
	compatFeatureDirectoryPreAllocate               feature = 0x1
	compatFeatureImagicInodes                       feature = 0x2
	compatFeatureHasJournal                         feature = 0x4
	compatFeatureExtendedAttributes                 feature = 0x8
	compatFeatureReservedGDTBlocksForExpansion      feature = 0x10
	compatFeatureDirectoryIndices                   feature = 0x20
	compatFeatureLazyBlockGroup                     feature = 0x40
	compatFeatureExcludeInode                       feature = 0x80
	compatFeatureExcludeBitmap                      feature = 0x100
	compatFeatureSparseSuperBlockV2                 feature = 0x200
	incompatFeatureCompression                      feature = 0x1
	incompatFeatureDirectoryEntriesRecordFileType   feature = 0x2
	incompatFeatureRecoveryNeeded                   feature = 0x4
	incompatFeatureSeparateJournalDevice            feature = 0x8
	incompatFeatureMetaBlockGroups                  feature = 0x10
	incompatFeatureExtents                          feature = 0x40
	incompatFeature64Bit                            feature = 0x80
	incompatFeatureMultipleMountProtection          feature = 0x100
	incompatFeatureFlexBlockGroups                  feature = 0x200
	incompatFeatureExtendedAttributeInodes          feature = 0x400
	incompatFeatureDataInDirectoryEntries           feature = 0x1000
	incompatFeatureMetadataChecksumSeedInSuperblock feature = 0x2000
	incompatFeatureLargeDirectory                   feature = 0x4000
	incompatFeatureDataInInode                      feature = 0x8000
	incompatFeatureEncryptInodes                    feature = 0x10000
	roCompatFeatureSparseSuperblock                 feature = 0x1
	roCompatFeatureLargeFile                        feature = 0x2
	roCompatFeatureBtreeDirectory                   feature = 0x4
	roCompatFeatureHugeFile                         feature = 0x8
	roCompatFeatureGDTChecksum                      feature = 0x10
	roCompatFeatureLargeSubdirectoryCount           feature = 0x20
	roCompatFeatureLargeInodes                      feature = 0x40
	roCompatFeatureSnapshot                         feature = 0x80
	roCompatFeatureQuota                            feature = 0x100
	roCompatFeatureBigalloc                         feature = 0x200
	roCompatFeatureMetadataChecksums                feature = 0x400
	roCompatFeatureReplicas                         feature = 0x800
	roCompatFeatureReadOnly                         feature = 0x1000
	roCompatFeatureProjectQuotas                    feature = 0x2000
	// hash algorithms for htree directory entries
	hashLegacy          hashAlgorithm = 0x0
	hashHalfMD4         hashAlgorithm = 0x1
	hashTea             hashAlgorithm = 0x2
	hashLegacyUnsigned  hashAlgorithm = 0x3
	hashHalfMD4Unsigned hashAlgorithm = 0x4
	hashTeaUnsigned     hashAlgorithm = 0x5
	// default mount options
	mountPrintDebugInfo                 mountOption = 0x1
	mountNewFilesGidContainingDirectory mountOption = 0x2
	mountUserspaceExtendedAttributes    mountOption = 0x4
	mountPosixACLs                      mountOption = 0x8
	mount16BitUIDs                      mountOption = 0x10
	mountJournalDataAndMetadata         mountOption = 0x20
	mountFlushBeforeJournal             mountOption = 0x40
	mountUnorderingDataMetadata         mountOption = 0x60
	mountDisableWriteFlushes            mountOption = 0x100
	mountTrackMetadataBlocks            mountOption = 0x200
	mountDiscardDeviceSupport           mountOption = 0x400
	mountDisableDelayedAllocation       mountOption = 0x800
	// miscellaneous flags
	flagSignedDirectoryHash   flag = 0x0001
	flagUnsignedDirectoryHash flag = 0x0002
	flagTestDevCode           flag = 0x0004
	// encryption algorithms
	encryptionAlgorithmInvalid   encryptionAlgorithm = 1
	encryptionAlgorithm256AESXTS encryptionAlgorithm = 2
	encryptionAlgorithm256AESGCM encryptionAlgorithm = 3
	encryptionAlgorithm256AESCBC encryptionAlgorithm = 4
)

// journalBackup is a backup in the superblock of the journal's inode i_block[] array and size
type journalBackup struct {
	iBlocks []uint32
	iSize   uint64
}

// mountOptions is a structure holding which default mount options are set
type mountOptions struct {
	printDebugInfo                 bool
	newFilesGidContainingDirectory bool
	userspaceExtendedAttributes    bool
	posixACLs                      bool
	use16BitUIDs                   bool
	journalDataAndMetadata         bool
	flushBeforeJournal             bool
	unorderingDataMetadata         bool
	disableWriteFlushes            bool
	trackMetadataBlocks            bool
	discardDeviceSupport           bool
	disableDelayedAllocation       bool
}

// miscFlags is a structure holding various miscellaneous flags
type miscFlags struct {
	signedDirectoryHash   bool
	unsignedDirectoryHash bool
	developmentTest       bool
}

// featureFlags is a structure holding which flags are set - compatible, incompatible and read-only compatible
type featureFlags struct {
	// compatible, incompatible, and compatibleReadOnly feature flags
	directoryPreAllocate             bool
	imagicInodes                     bool
	hasJournal                       bool
	extendedAttributes               bool
	reservedGDTBlocksForExpansion    bool
	directoryIndices                 bool
	lazyBlockGroup                   bool
	excludeInode                     bool
	excludeBitmap                    bool
	sparseSuperBlockV2               bool
	compression                      bool
	directoryEntriesRecordFileType   bool
	recoveryNeeded                   bool
	separateJournalDevice            bool
	metaBlockGroups                  bool
	extents                          bool
	fs64Bit                          bool
	multipleMountProtection          bool
	flexBlockGroups                  bool
	extendedAttributeInodes          bool
	dataInDirectoryEntries           bool
	metadataChecksumSeedInSuperblock bool
	largeDirectory                   bool
	dataInInode                      bool
	encryptInodes                    bool
	sparseSuperblock                 bool
	largeFile                        bool
	btreeDirectory                   bool
	hugeFile                         bool
	gdtChecksum                      bool
	largeSubdirectoryCount           bool
	largeInodes                      bool
	snapshot                         bool
	quota                            bool
	bigalloc                         bool
	metadataChecksums                bool
	replicas                         bool
	readOnly                         bool
	projectQuotas                    bool
}

// Superblock is a structure holding the ext4 superblock
type superblock struct {
	inodeCount                   uint32
	blockCount                   uint64
	reservedBlocks               uint64
	freeBlocks                   uint64
	freeInodes                   uint32
	firstDataBlock               uint32
	blockSize                    uint64
	clusterSize                  uint64
	blocksPerGroup               uint32
	clustersPerGroup             uint32
	inodesPerGroup               uint32
	mountTime                    uint32
	writeTime                    uint32
	mountCount                   uint16
	mountsToFsck                 uint16
	filesystemState              filesystemState
	errorBehaviour               errorBehaviour
	minorRevision                uint16
	lastCheck                    uint32
	checkInterval                uint32
	creatorOS                    osFlag
	revisionLevel                uint32
	reservedBlocksDefaultUID     uint16
	reservedBlocksDefaultGID     uint16
	firstNonReservedInode        uint32
	inodeSize                    uint16
	blockGroup                   uint16
	features                     featureFlags
	uuid                         string
	volumeLabel                  string
	lastMountedDirectory         string
	algorithmUsageBitmap         uint32
	preallocationBlocks          byte
	preallocationDirectoryBlocks byte
	reservedGDTBlocks            uint16
	journalSuperblockUUID        string
	journalInode                 uint32
	journalDeviceNumber          uint32
	orphanedInodesStart          uint32
	hashTreeSeed                 []uint32
	hashVersion                  hashAlgorithm
	groupDescriptorSize          uint16
	defaultMountOptions          mountOptions
	firstMetablockGroup          uint32
	mkfsTime                     uint32
	journalBackup                journalBackup
	// 64-bit mode features
	inodeMinBytes                uint16
	inodeReserveBytes            uint16
	miscFlags                    miscFlags
	raidStride                   uint16
	multiMountPreventionInterval uint16
	multiMountProtectionBlock    uint64
	raidStripeWidth              uint32
	logGroupsPerFlex             uint64
	checksumType                 byte
	totalKBWritten               uint64
	snapshotInodeNumber          uint32
	snapshotID                   uint32
	snapshotReservedBlocks       uint64
	snapshotStartInode           uint32
	errorCount                   uint32
	errorFirstTime               uint32
	errorFirstInode              uint32
	errorFirstBlock              uint64
	errorFirstFunction           string
	errorFirstLine               uint32
	errorLastTime                uint32
	errorLastInode               uint32
	errorLastLine                uint32
	errorLastBlock               uint64
	errorLastFunction            string
	mountOptions                 string
	userQuotaInode               uint32
	groupQuotaInode              uint32
	overheadBlocks               uint32
	backupSuperblockBlockGroups  []uint32
	encryptionAlgorithms         []encryptionAlgorithm
	encryptionSalt               []byte
	lostFoundInode               uint32
	projectQuotaInode            uint32
	checksumSeed                 uint32
	checksum                     uint32
}

func (sb *superblock) equal(a *superblock) bool {
	if (sb == nil && a != nil) || (a == nil && sb != nil) {
		return false
	}
	if sb == nil && a == nil {
		return true
	}
	return *sb == *a
}

// FSInformationSectorFromBytes create an FSInformationSector struct from bytes
func superblockFromBytes(b []byte) (*superblock, error) {
	bLen := len(b)
	if bLen != int(SuperblockSize) {
		return nil, fmt.Errorf("Cannot read superblock from %d bytes instead of expected %d", bLen, SuperblockSize)
	}

	// check the magic signature
	actualSignature := binary.LittleEndian.Uint16(b[0x38:0x3a])
	if actualSignature != superblockSignature {
		return nil, fmt.Errorf("Erroneous signature at location 0x38 was %x instead of expected %x", actualSignature, superblockSignature)
	}

	sb := superblock{}

	// first read feature flags of various types
	compatFlags := binary.LittleEndian.Uint32(b[0x5c:0x60])
	incompatFlags := binary.LittleEndian.Uint32(b[0x60:0x64])
	roCompatFlags := binary.LittleEndian.Uint32(b[0x64:0x68])
	// track which ones are set
	sb.features = parseFeatureFlags(compatFlags, incompatFlags, roCompatFlags)

	sb.inodeCount = binary.LittleEndian.Uint32(b[0:4])

	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	blockCount := make([]byte, 8, 8)
	reservedBlocks := make([]byte, 8, 8)
	freeBlocks := make([]byte, 8, 8)

	copy(blockCount[0:4], b[0x4:0x8])
	copy(reservedBlocks[0:4], b[0x8:0xc])
	copy(freeBlocks[0:4], b[0xc:0x10])

	if sb.features.fs64Bit {
		copy(blockCount[4:8], b[0x150:0x154])
		copy(reservedBlocks[4:8], b[0x154:0x158])
		copy(freeBlocks[4:8], b[0x158:0x15c])
	}
	sb.blockCount = binary.LittleEndian.Uint64(blockCount)
	sb.reservedBlocks = binary.LittleEndian.Uint64(reservedBlocks)
	sb.freeBlocks = binary.LittleEndian.Uint64(freeBlocks)

	sb.freeInodes = binary.LittleEndian.Uint32(b[0x10:0x14])
	sb.firstDataBlock = binary.LittleEndian.Uint32(b[0x14:0x18])
	sb.blockSize = (uint64).math.Exp2(10 + binary.LittleEndian.Uint32(b[0x18:0x1c]))
	sb.clusterSize = (uint64).math.Exp2(binary.LittleEndian.Uint32(b[0x1c:0x20]))
	sb.blocksPerGroup = binary.LittleEndian.Uint32(b[0x20:0x24])
	if sb.features.bigalloc {
		sb.clustersPerGroup = binary.LittleEndian.Uint32(b[0x24:0x28])
	}
	sb.inodesPerGroup = binary.LittleEndian.Uint32(b[0x28:0x2c])
	sb.mountTime = binary.LittleEndian.Uint32(b[0x2c:0x30])
	sb.writeTime = binary.LittleEndian.Uint32(b[0x30:0x34])
	sb.mountCount = binary.LittleEndian.Uint16(b[0x34:0x36])
	sb.mountsToFsck = binary.LittleEndian.Uint16(b[0x36:0x38])

	sb.filesystemState = binary.LittleEndian.Uint16(b[0x3a:0x3c])
	sb.errorBehaviour = binary.LittleEndian.Uint16(b[0x3c:0x3e])

	sb.minorRevision = binary.LittleEndian.Uint16(b[0x3e:0x40])
	sb.lastCheck = binary.LittleEndian.Uint32(b[0x40:0x44])
	sb.checkInterval = binary.LittleEndian.Uint32(b[0x44:0x48])

	sb.creatorOS = binary.LittleEndian.Uint32(b[0x48:0x4c])
	sb.revisionLevel = binary.LittleEndian.Uint32(b[0x4c:0x50])
	sb.reservedBlocksDefaultUID = binary.LittleEndian.Uint16(b[0x50:0x52])
	sb.reservedBlocksDefaultGID = binary.LittleEndian.Uint16(b[0x52:0x54])

	sb.firstNonReservedInode = binary.LittleEndian.Uint32(b[0x54:0x58])
	sb.inodeSize = binary.LittleEndian.Uint16(b[0x58:0x5a])
	sb.blockGroup = binary.LittleEndian.Uint16(b[0x5a:0x5c])

	voluuid, err := uuid.FromBytes(bytesToUUIDBytes(b[0x68:0x78]))
	if err != nil {
		return nil, fmt.Errorf("unable to read volume UUID: %v", err)
	}
	sb.uuid = voluuid.String()
	sb.volumeLabel = string(b[0x78:0x88])
	sb.lastMountedDirectory = string(b[0x88:0xc8])
	sb.algorithmUsageBitmap = binary.LittleEndian.Uint32(b[0xc8:0xcc])

	sb.preallocationBlocks = b[0xcc:0xcd]
	sb.preallocationDirectoryBlocks = b[0xcd:0xce]
	sb.reservedGDTBlocks = binary.LittleEndian.Uint16(b[0xce:0xd0])

	journaluuid, err := uuid.FromBytes(bytesToUUIDBytes(b[0xd0:0xe0]))
	if err != nil {
		return nil, fmt.Errorf("unable to read journal UUID: %v", err)
	}
	sb.journalSuperblockUUID = journaluuid.String()
	sb.journalInode = binary.LittleEndian.Uint32(b[0xe0:0xe4])
	sb.journalDeviceNumber = binary.LittleEndian.Uint32(b[0xe4:0xe8])
	sb.orphanedInodesStart = binary.LittleEndian.Uint32(b[0xe8:0xec])

	htreeSeed := make([]uint32, 0, 4)
	htreeSeed = append(htreeSeed, binary.LittleEndian.Uint32(b[0xec:0xf0]))
	htreeSeed = append(htreeSeed, binary.LittleEndian.Uint32(b[0xf0:0xf4]))
	htreeSeed = append(htreeSeed, binary.LittleEndian.Uint32(b[0xf4:0xf8]))
	htreeSeed = append(htreeSeed, binary.LittleEndian.Uint32(b[0xf8:0xfc]))
	sb.hashTreeSeed = htreeSeed

	sb.hashVersion = hashAlgorithm(b[0xfc:0xfd])

	sb.groupDescriptorSize = binary.LittleEndian.Uint16(b[0xfd:0x100])

	sb.defaultMountOptions = parseMountOptions(binary.LittleEndian.Uint32(b[0x100:0x104]))
	sb.firstMetablockGroup = binary.LittleEndian.Uint32(b[0x104:0x108])
	sb.mkfsTime = binary.LittleEndian.Uint32(b[0x108:0x10c])

	journalBackupType := b[0xfd]
	if journalBackupType == 0 {
		journalBackupArray := make([]uint32, 0, 15)
		startJournalBackup := 0x10c
		for i := 0; i < 15; i++ {
			start := startJournalBackup + 4*i
			end := startJournalBackup + 4*i + 4
			journalBackupArray = append(journalBackupArray, binary.LittleEndian.Uint32(b[start:end]))
		}
		iSizeBytes := make([]byte, 8, 8)

		copy(iSizeBytes[0:4], b[startJournalBackup+4*16:startJournalBackup+4*17])
		copy(iSizeBytes[4:8], b[startJournalBackup+4*15:startJournalBackup+4*16])

		sb.journalBackup = journalBackup{
			iSize:   binary.LittleEndian.Uint64(iSizeBytes),
			iBlocks: journalBackupArray,
		}
	}

	sb.inodeMinBytes = binary.LittleEndian.Uint16(b[0x15c:0x15e])
	sb.inodeReserveBytes = binary.LittleEndian.Uint16(b[0x15e:0x160])
	sb.miscFlags = parseMiscFlags(binary.LittleEndian.Uint32(b[0x160:0x164]))

	sb.raidStride = binary.LittleEndian.Uint16(b[0x164:0x166])
	sb.raidStripeWidth = binary.LittleEndian.Uint32(b[0x170:0x174])

	sb.multiMountPreventionInterval = binary.LittleEndian.Uint16(b[0x166:0x168])
	sb.multiMountProtectionBlock = binary.LittleEndian.Uint64(b[0x168:0x170])

	sb.logGroupsPerFlex = (uint64).math.Exp2(b[0x174])

	sb.checksumType = b[0x175] // only valid one is 1
	if sb.checksumType != crc32c {
		return nil, fmt.Errorf("Cannot read superblock: invalid checksum type %d, only valid is %d", sb.checksumType, crc32c)
	}

	// b[0x176:0x178] are reserved padding

	sb.totalKBWritten = binary.LittleEndian.Uint64(b[0x178:0x180])

	sb.snapshotInodeNumber = binary.LittleEndian.Uint32(b[0x180:0x184])
	sb.snapshotID = binary.LittleEndian.Uint32(b[0x184:0x188])
	sb.snapshotReservedBlocks = binary.LittleEndian.Uint64(b[0x188:0x190])
	sb.snapshotStartInode = binary.LittleEndian.Uint32(b[0x190:0x194])

	// errors
	sb.errorCount = binary.LittleEndian.Uint32(b[0x194:0x198])
	sb.errorFirstTime = binary.LittleEndian.Uint32(b[0x198:0x19c])
	sb.errorFirstInode = binary.LittleEndian.Uint32(b[0x19c:0x1a0])
	sb.errorFirstBlock = binary.LittleEndian.Uint64(b[0x1a0:0x1a8])
	sb.errorFirstFunction = string(b[0x1a8:0x1c8])
	sb.errorFirstLine = binary.LittleEndian.Uint32(b[0x1c8:0x1cc])
	sb.errorLastTime = binary.LittleEndian.Uint32(b[0x1cc:0x1d0])
	sb.errorLastInode = binary.LittleEndian.Uint32(b[0x1d0:0x1d4])
	sb.errorLastLine = binary.LittleEndian.Uint32(b[0x1d4:0x1d8])
	sb.errorLastBlock = binary.LittleEndian.Uint64(b[0x1d8:0x1e0])
	sb.errorLastFunction = string(b[0x1e0:0x200])

	sb.mountOptions = string(b[0x200:0x240])
	sb.userQuotaInode = binary.LittleEndian.Uint32(b[0x240:0x244])
	sb.groupQuotaInode = binary.LittleEndian.Uint32(b[0x244:0x248])
	// overheadBlocks *always* is 0
	sb.overheadBlocks = binary.LittleEndian.Uint32(b[0x248:0x24c])
	sb.backupSuperblockBlockGroups = []uint32{
		binary.LittleEndian.Uint32(b[0x24c:0x250]),
		binary.LittleEndian.Uint32(b[0x250:0x254]),
	}
	sb.encryptionAlgorithms = b[0x254:0x258]
	sb.encryptionSalt = b[0x258:0x268]
	sb.lostFoundInode = binary.LittleEndian.Uint32(b[0x268:0x26c])
	sb.projectQuotaInode = binary.LittleEndian.Uint32(b[0x26c:0x270])

	sb.checksumSeed = binary.LittleEndian.Uint32(b[0x270:0x274])

	// b[0x274:0x3fc] are reserved for zero padding

	// checksum
	sb.checksum = binary.LittleEndian.Uint32(b[0x3fc:0x400])

	// calculate the checksum and validate - we use crc32c
	if sb.features.metadataChecksums {
		crc32Table := crc32.MakeTable(crc32.Castagnoli)
		actualChecksum := crc32.Checksum(b[0:0x3fe], crc32Table)
		if actualChecksum != sb.checksum {
			return nil, fmt.Errorf("Invalid superblock checksum, actual was %x, on disk was %x", actualChecksum, sb.checksum)
		}
	}

	return &sb, nil
}

// toBytes returns a superblock ready to be written to disk
func (sb *superblock) toBytes() ([]byte, error) {
	b := make([]byte, SuperblockSize, SuperblockSize)

	binary.LittleEndian.PutUint16(b[0x38:0x3a], superblockSignature)
	compatFlags, incompatFlags, roCompatFlags := sb.features.toInts()
	binary.LittleEndian.PutUint32(b[0x5c:0x60], compatFlags)
	binary.LittleEndian.PutUint32(b[0x60:0x64], incompatFlags)
	binary.LittleEndian.PutUint32(b[0x64:0x68], roCompatFlags)

	binary.LittleEndian.PutUint32(b[0:4], sb.inodeCount)

	// block count, reserved block count and free blocks depends on whether the fs is 64-bit or not
	blockCount := make([]byte, 8, 8)
	reservedBlocks := make([]byte, 8, 8)
	freeBlocks := make([]byte, 8, 8)

	binary.LittleEndian.PutUint64(blockCount, sb.blockCount)
	binary.LittleEndian.PutUint64(reservedBlocks, sb.reservedBlocks)
	binary.LittleEndian.PutUint64(freeBlocks, sb.freeBlocks)

	copy(b[0x4:0x8], blockCount[0:4])
	copy(b[0x8:0xc], reservedBlocks[0:4])
	copy(b[0xc:0x10], freeBlocks[0:4])

	if sb.features.fs64Bit {
		copy(b[0x150:0x154], blockCount[4:8])
		copy(b[0x154:0x158], reservedBlocks[4:8])
		copy(b[0x158:0x15c], freeBlocks[4:8])
	}

	binary.LittleEndian.PutUint32(b[0x10:0x14], sb.freeInodes)
	binary.LittleEndian.PutUint32(b[0x14:0x18], sb.firstDataBlock)
	binary.LittleEndian.PutUint32(b[0x18:0x1c], (uint32).math.Log2(sb.blockSize)-10)
	binary.LittleEndian.PutUint32(b[0x1c:0x20], (uint32).math.Log2(sb.clusterSize))

	binary.LittleEndian.PutUint32(b[0x20:0x24], sb.blocksPerGroup)
	if sb.features.bigalloc {
		binary.LittleEndian.PutUint32(b[0x24:0x28], sb.clustersPerGroup)
	}
	binary.LittleEndian.PutUint32(b[0x28:0x2c], sb.inodesPerGroup)
	binary.LittleEndian.PutUint32(b[0x2c:0x30], sb.mountTime)
	binary.LittleEndian.PutUint32(b[0x30:0x34], sb.writeTime)
	binary.LittleEndian.PutUint16(b[0x34:0x36], sb.mountCount)
	binary.LittleEndian.PutUint16(b[0x36:0x38], sb.mountsToFsck)

	binary.LittleEndian.PutUint16(b[0x3a:0x3c], sb.filesystemState)
	binary.LittleEndian.PutUint16(b[0x3c:0x3e], sb.errorBehaviour)

	binary.LittleEndian.PutUint16(b[0x3e:0x40], sb.minorRevision)
	binary.LittleEndian.PutUint32(b[0x40:0x44], sb.lastCheck)
	binary.LittleEndian.PutUint32(b[0x44:0x48], sb.checkInterval)

	binary.LittleEndian.PutUint32(b[0x48:0x4c], sb.creatorOS)
	binary.LittleEndian.PutUint32(b[0x4c:0x50], sb.revisionLevel)
	binary.LittleEndian.PutUint16(b[0x50:0x52], sb.reservedBlocksDefaultUID)
	binary.LittleEndian.PutUint16(b[0x52:0x54], sb.reservedBlocksDefaultGID)

	binary.LittleEndian.PutUint32(b[0x54:0x58], sb.firstNonReservedInode)
	binary.LittleEndian.PutUint16(b[0x58:0x5a], sb.inodeSize)
	binary.LittleEndian.PutUint16(b[0x5a:0x5c], sb.blockGroup)

	uuidBytes, err := uuid.FromString(sb.uuid)
	if err != nil {
		return nil, fmt.Errorf("Invalid volume UUID: %s", sb.uuid)
	}
	copy(b[0x68:0x78], bytesToUUIDBytes(uuidBytes.Bytes()))

	ab, err := stringToASCIIBytes(sb.volumeLabel)
	if err != nil {
		return nil, fmt.Errorf("Error converting volume label to bytes: %v", err)
	}
	copy(b[0x78:0x88], ab[0:16])
	ab, err = stringToASCIIBytes(sb.lastMountedDirectory)
	if err != nil {
		return nil, fmt.Errorf("Error last mounted directory to bytes: %v", err)
	}
	copy(b[0x88:0xc8], ab[0:64])

	binary.LittleEndian.PutUint32(b[0xc8:0xcc], sb.algorithmUsageBitmap)

	b[0xcc] = sb.preallocationBlocks
	b[0xcd] = sb.preallocationDirectoryBlocks
	binary.LittleEndian.PutUint16(b[0xce:0xd0], sb.reservedGDTBlocks)

	uuidBytes, err = uuid.FromString(sb.journalSuperblockUUID)
	if err != nil {
		return nil, fmt.Errorf("Invalid journal superblock UUID: %s", sb.uuid)
	}
	copy(b[0xd0:0xe0], bytesToUUIDBytes(uuidBytes.Bytes()))

	binary.LittleEndian.PutUint32(b[0xe0:0xe4], sb.journalInode)
	binary.LittleEndian.PutUint32(b[0xe4:0xe8], sb.journalDeviceNumber)
	binary.LittleEndian.PutUint32(b[0xe8:0xec], sb.orphanedInodesStart)

	binary.LittleEndian.PutUint32(b[0xec:0xf0], sb.hashTreeSeed[0])
	binary.LittleEndian.PutUint32(b[0xf0:0xf4], sb.hashTreeSeed[1])
	binary.LittleEndian.PutUint32(b[0xf4:0xf8], sb.hashTreeSeed[2])
	binary.LittleEndian.PutUint32(b[0xf8:0xfc], sb.hashTreeSeed[3])

	b[0xfc] = sb.hashVersion

	binary.LittleEndian.PutUint16(b[0xfd:0x100], sb.groupDescriptorSize)

	binary.LittleEndian.PutUint32(b[0x100:0x104], sb.defaultMountOptions.toInt())
	binary.LittleEndian.PutUint32(b[0x104:0x108], sb.firstMetablockGroup)
	binary.LittleEndian.PutUint32(b[0x108:0x10c], sb.mkfsTime)

	if sb.journalBackup != nil {
		b[0xfd] = 0
		startJournalBackup := 0x10c
		for i := 0; i < 15; i++ {
			start := startJournalBackup + 4*i
			end := startJournalBackup + 4*i + 4
			binary.LittleEndian.PutUint32(b[start:end], sb.journalBackup.iBlocks[i])
		}

		iSizeBytes := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(iSizeBytes, sb.journalBackup.iSize)
		copy(b[startJournalBackup+4*16:startJournalBackup+4*17], iSizeBytes[0:4])
		copy(b[startJournalBackup+4*15:startJournalBackup+4*16], iSizeBytes[4:8])
	}

	binary.LittleEndian.PutUint16(b[0x15c:0x15e], sb.inodeMinBytes)
	binary.LittleEndian.PutUint16(b[0x15e:0x160], sb.inodeReserveBytes)
	binary.LittleEndian.PutUint32(b[0x160:0x164], sb.miscFlags.toInt())

	binary.LittleEndian.PutUint16(b[0x164:0x166], sb.raidStride)
	binary.LittleEndian.PutUint32(b[0x170:0x174], sb.raidStripeWidth)

	binary.LittleEndian.PutUint16(b[0x166:0x168], sb.multiMountPreventionInterval)
	binary.LittleEndian.PutUint64(b[0x168:0x170], sb.multiMountProtectionBlock)

	b[0x174] = (byte).math.Log2(sb.logGroupsPerFlex)

	b[0x175] = sb.checksumType // only valid one is 1

	// b[0x176:0x178] are reserved padding

	binary.LittleEndian.PutUint64(b[0x178:0x180], sb.totalKBWritten)

	binary.LittleEndian.PutUint32(b[0x180:0x184], sb.snapshotInodeNumber)
	binary.LittleEndian.PutUint32(b[0x184:0x188], sb.snapshotID)
	binary.LittleEndian.PutUint64(b[0x188:0x190], sb.snapshotReservedBlocks)
	binary.LittleEndian.PutUint32(b[0x190:0x194], sb.snapshotStartInode)

	// errors
	binary.LittleEndian.PutUint32(b[0x194:0x198], sb.errorCount)
	binary.LittleEndian.PutUint32(b[0x198:0x19c], sb.errorFirstTime)
	binary.LittleEndian.PutUint32(b[0x19c:0x1a0], sb.errorFirstInode)
	binary.LittleEndian.PutUint64(b[0x1a0:0x1a8], sb.errorFirstBlock)
	copy(b[0x1a8:0x1c8], stringToASCIIBytes(sb.errorFirstFunction))
	binary.LittleEndian.PutUint32(b[0x1c8:0x1cc], sb.errorFirstLine)
	binary.LittleEndian.PutUint32(b[0x1cc:0x1d0], sb.errorLastTime)
	binary.LittleEndian.PutUint32(b[0x1d0:0x1d4], sb.errorLastInode)
	binary.LittleEndian.PutUint32(b[0x1d4:0x1d8], sb.errorLastLine)
	binary.LittleEndian.PutUint64(b[0x1d8:0x1e0], sb.errorLastBlock)
	copy(b[0x1e0:0x200], stringToASCIIBytes(sb.errorLastFunction))

	copy(b[0x200:0x240], stringToASCIIBytes(sb.mountOptions))
	binary.LittleEndian.PutUint32(b[0x240:0x244], sb.userQuotaInode)
	binary.LittleEndian.PutUint32(b[0x244:0x248], sb.groupQuotaInode)
	// overheadBlocks *always* is 0
	binary.LittleEndian.PutUint32(b[0x248:0x24c], sb.overheadBlocks)
	binary.LittleEndian.PutUint32(b[0x24c:0x250], sb.backupSuperblockBlockGroups[0])
	binary.LittleEndian.PutUint32(b[0x250:0x254], sb.backupSuperblockBlockGroups[1])
	copy(b[0x254:0x258], sb.encryptionAlgorithms)
	copy(b[0x258:0x268], sb.encryptionSalt)
	binary.LittleEndian.PutUint32(b[0x268:0x26c], sb.lostFoundInode)
	binary.LittleEndian.PutUint32(b[0x26c:0x270], sb.projectQuotaInode)

	binary.LittleEndian.PutUint32(b[0x270:0x274], sb.checksumSeed)

	// b[0x274:0x3fc] are reserved for zero padding

	// calculate the checksum and validate - we use crc32c
	if sb.features.metadataChecksums {
		crc32Table := crc32.MakeTable(crc32.Castagnoli)
		actualChecksum := crc32.Checksum(b[0:0x3fe], crc32Table)
		binary.LittleEndian.PutUint32(b[0x3fc:0x400], actualChecksum)
	}

	return b, nil
}

func parseFeatureFlags(compatFlags, incompatFlags, roCompatFlags uint32) featureFlags {
	f := featureFlags{
		directoryPreAllocate:             compatFlags&compatFeatureDirectoryPreAllocate == compatFeatureDirectoryPreAllocate,
		imagicInodes:                     compatFlags&compatFeatureImagicInodes == compatFeatureImagicInodes,
		hasJournal:                       compatFlags&compatFeatureHasJournal == compatFeatureHasJournal,
		extendedAttributes:               compatFlags&compatFeatureExtendedAttributes == compatFeatureExtendedAttributes,
		reservedGDTBlocksForExpansion:    compatFlags&compatFeatureReservedGDTBlocksForExpansion == compatFeatureReservedGDTBlocksForExpansion,
		directoryIndices:                 compatFlags&compatFeatureDirectoryIndices == compatFeatureDirectoryIndices,
		lazyBlockGroup:                   compatFlags&compatFeatureLazyBlockGroup == compatFeatureLazyBlockGroup,
		excludeInode:                     compatFlags&compatFeatureExcludeInode == compatFeatureExcludeInode,
		excludeBitmap:                    compatFlags&compatFeatureExcludeBitmap == compatFeatureExcludeBitmap,
		sparseSuperBlockV2:               compatFlags&compatFeatureSparseSuperBlockV2 == compatFeatureSparseSuperBlockV2,
		compression:                      incompatFlags&incompatFeatureCompression == incompatFeatureCompression,
		directoryEntriesRecordFileType:   incompatFlags&incompatFeatureDirectoryEntriesRecordFileType == incompatFeatureDirectoryEntriesRecordFileType,
		recoveryNeeded:                   incompatFlags&incompatFeatureRecoveryNeeded == incompatFeatureRecoveryNeeded,
		separateJournalDevice:            incompatFlags&incompatFeatureSeparateJournalDevice == incompatFeatureSeparateJournalDevice,
		metaBlockGroups:                  incompatFlags&incompatFeatureMetaBlockGroups == incompatFeatureMetaBlockGroups,
		extents:                          incompatFlags&incompatFeatureExtents == incompatFeatureExtents,
		fs64Bit:                          incompatFlags&incompatFeature64Bit == incompatFeature64Bit,
		multipleMountProtection:          incompatFlags&incompatFeatureMultipleMountProtection == incompatFeatureMultipleMountProtection,
		flexBlockGroups:                  incompatFlags&incompatFeatureFlexBlockGroups == incompatFeatureFlexBlockGroups,
		extendedAttributeInodes:          incompatFlags&incompatFeatureExtendedAttributeInodes == incompatFeatureExtendedAttributeInodes,
		dataInDirectoryEntries:           incompatFlags&incompatFeatureDataInDirectoryEntries == incompatFeatureDataInDirectoryEntries,
		metadataChecksumSeedInSuperblock: incompatFlags&incompatFeatureMetadataChecksumSeedInSuperblock == incompatFeatureMetadataChecksumSeedInSuperblock,
		largeDirectory:                   incompatFlags&incompatFeatureLargeDirectory == incompatFeatureLargeDirectory,
		dataInInode:                      incompatFlags&incompatFeatureDataInInode == incompatFeatureDataInInode,
		encryptInodes:                    incompatFlags&incompatFeatureEncryptInodes == incompatFeatureEncryptInodes,
		sparseSuperblock:                 roCompatFlags&roCompatFeatureSparseSuperblock == roCompatFeatureSparseSuperblock,
		largeFile:                        roCompatFlags&roCompatFeatureLargeFile == roCompatFeatureLargeFile,
		btreeDirectory:                   roCompatFlags&roCompatFeatureBtreeDirectory == roCompatFeatureBtreeDirectory,
		hugeFile:                         roCompatFlags&roCompatFeatureHugeFile == roCompatFeatureHugeFile,
		gdtChecksum:                      roCompatFlags&roCompatFeatureGDTChecksum == roCompatFeatureGDTChecksum,
		largeSubdirectoryCount:           roCompatFlags&roCompatFeatureLargeSubdirectoryCount == roCompatFeatureLargeSubdirectoryCount,
		largeInodes:                      roCompatFlags&roCompatFeatureLargeInodes == roCompatFeatureLargeInodes,
		snapshot:                         roCompatFlags&roCompatFeatureSnapshot == roCompatFeatureSnapshot,
		quota:                            roCompatFlags&roCompatFeatureQuota == roCompatFeatureQuota,
		bigalloc:                         roCompatFlags&roCompatFeatureBigalloc == roCompatFeatureBigalloc,
		metadataChecksums:                roCompatFlags&roCompatFeatureMetadataChecksums == roCompatFeatureMetadataChecksums,
		replicas:                         roCompatFlags&roCompatFeatureReplicas == roCompatFeatureReplicas,
		readOnly:                         roCompatFlags&roCompatFeatureReadOnly == roCompatFeatureReadOnly,
		projectQuotas:                    roCompatFlags&roCompatFeatureProjectQuotas == roCompatFeatureProjectQuotas,
	}

	return f
}

func (f *featureFlags) toInts() (uint32, uint32, uint32) {
	var (
		compatFlags    uint32
		inccompatFlags uint32
		roCompatFlags  uint32
	)

	// compatible flags
	if f.directoryPreAllocate {
		compatFlags = compatFlags | compatFeatureDirectoryPreAllocate
	}
	if f.imagicInodes {
		compatFlags = compatFlags | compatFeatureImagicInodes
	}
	if f.hasJournal {
		compatFlags = compatFlags | compatFeatureHasJournal
	}
	if f.extendedAttributes {
		compatFlags = compatFlags | compatFeatureExtendedAttributes
	}
	if f.reservedGDTBlocksForExpansion {
		compatFlags = compatFlags | compatFeatureReservedGDTBlocksForExpansion
	}
	if f.directoryIndices {
		compatFlags = compatFlags | compatFeatureDirectoryIndices
	}
	if f.lazyBlockGroup {
		compatFlags = compatFlags | compatFeatureLazyBlockGroup
	}
	if f.excludeInode {
		compatFlags = compatFlags | compatFeatureExcludeInode
	}
	if f.excludeBitmap {
		compatFlags = compatFlags | compatFeatureExcludeBitmap
	}
	if f.sparseSuperBlockV2 {
		compatFlags = compatFlags | compatFeatureSparseSuperBlockV2
	}

	// incompatible flags
	if f.compression {
		incompatFlags = incompatFlags | incompatFeatureCompression
	}
	if f.directoryEntriesRecordFileType {
		incompatFlags = incompatFlags | incompatFeatureDirectoryEntriesRecordFileType
	}
	if f.recoveryNeeded {
		incompatFlags = incompatFlags | incompatFeatureRecoveryNeeded
	}
	if f.separateJournalDevice {
		incompatFlags = incompatFlags | incompatFeatureSeparateJournalDevice
	}
	if f.metaBlockGroups {
		incompatFlags = incompatFlags | incompatFeatureMetaBlockGroups
	}
	if f.extents {
		incompatFlags = incompatFlags | incompatFeatureExtents
	}
	if f.fs64Bit {
		incompatFlags = incompatFlags | incompatFeature64Bit
	}
	if f.multipleMountProtection {
		incompatFlags = incompatFlags | incompatFeatureMultipleMountProtection
	}
	if f.flexBlockGroups {
		incompatFlags = incompatFlags | incompatFeatureFlexBlockGroups
	}
	if f.extendedAttributeInodes {
		incompatFlags = incompatFlags | incompatFeatureExtendedAttributeInodes
	}
	if f.dataInDirectoryEntries {
		incompatFlags = incompatFlags | incompatFeatureDataInDirectoryEntries
	}
	if f.metadataChecksumSeedInSuperblock {
		incompatFlags = incompatFlags | incompatFeatureMetadataChecksumSeedInSuperblock
	}
	if f.largeDirectory {
		incompatFlags = incompatFlags | incompatFeatureLargeDirectory
	}
	if f.dataInInode {
		incompatFlags = incompatFlags | incompatFeatureDataInInode
	}
	if f.encryptInodes {
		incompatFlags = incompatFlags | incompatFeatureEncryptInodes
	}

	// read only compatible flags
	if f.sparseSuperblock {
		roCompatFlags = roCompatFlags | roCompatFeatureSparseSuperblock
	}
	if f.largeFile {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeFile
	}
	if f.btreeDirectory {
		roCompatFlags = roCompatFlags | roCompatFeatureBtreeDirectory
	}
	if f.hugeFile {
		roCompatFlags = roCompatFlags | roCompatFeatureHugeFile
	}
	if f.gdtChecksum {
		roCompatFlags = roCompatFlags | roCompatFeatureGDTChecksum
	}
	if f.largeSubdirectoryCount {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeSubdirectoryCount
	}
	if f.largeInodes {
		roCompatFlags = roCompatFlags | roCompatFeatureLargeInodes
	}
	if f.snapshot {
		roCompatFlags = roCompatFlags | roCompatFeatureSnapshot
	}
	if f.quota {
		roCompatFlags = roCompatFlags | roCompatFeatureQuota
	}
	if f.bigalloc {
		roCompatFlags = roCompatFlags | roCompatFeatureBigalloc
	}
	if f.metadataChecksums {
		roCompatFlags = roCompatFlags | roCompatFeatureMetadataChecksums
	}
	if f.replicas {
		roCompatFlags = roCompatFlags | roCompatFeatureReplicas
	}
	if f.readOnly {
		roCompatFlags = roCompatFlags | roCompatFeatureReadOnly
	}
	if f.projectQuotas {
		roCompatFlags = roCompatFlags | roCompatFeatureProjectQuotas
	}

	return compatFlags, incompatFlags, roCompatFlags
}

func parseMountOptions(flags uint32) mountOptions {
	m := mountOptions{
		printDebugInfo:                 flags&mountPrintDebugInfo == mountPrintDebugInfo,
		newFilesGidContainingDirectory: flags&mountNewFilesGidContainingDirectory == mountNewFilesGidContainingDirectory,
		userspaceExtendedAttributes:    flags&mountUserspaceExtendedAttributes == mountUserspaceExtendedAttributes,
		posixACLs:                      flags&mountPosixACLs == mountPosixACLs,
		use16BitUIDs:                   flags&mount16BitUIDs == mount16BitUIDs,
		journalDataAndMetadata:         flags&mountJournalDataAndMetadata == mountJournalDataAndMetadata,
		flushBeforeJournal:             flags&mountFlushBeforeJournal == mountFlushBeforeJournal,
		unorderingDataMetadata:         flags&mountUnorderingDataMetadata == mountUnorderingDataMetadata,
		disableWriteFlushes:            flags&mountDisableWriteFlushes == mountDisableWriteFlushes,
		trackMetadataBlocks:            flags&mountTrackMetadataBlocks == mountTrackMetadataBlocks,
		discardDeviceSupport:           flags&mountDiscardDeviceSupport == mountDiscardDeviceSupport,
		disableDelayedAllocation:       flags&mountDisableDelayedAllocation == mountDisableDelayedAllocation,
	}
	return m
}

func (m *mountOptions) toInt() uint32 {
	var flags uint32

	if m.printDebugInfo {
		flags = flags | mountPrintDebugInfo
	}
	if m.newFilesGidContainingDirectory {
		flags = flags | mountNewFilesGidContainingDirectory
	}
	if m.userspaceExtendedAttributes {
		flags = flags | mountUserspaceExtendedAttributes
	}
	if m.posixACLs {
		flags = flags | mountPosixACLs
	}
	if m.use16BitUIDs {
		flags = flags | mount16BitUIDs
	}
	if m.journalDataAndMetadata {
		flags = flags | mountJournalDataAndMetadata
	}
	if m.flushBeforeJournal {
		flags = flags | mountFlushBeforeJournal
	}
	if m.unorderingDataMetadata {
		flags = flags | mountUnorderingDataMetadata
	}
	if m.disableWriteFlushes {
		flags = flags | mountDisableWriteFlushes
	}
	if m.trackMetadataBlocks {
		flags = flags | mountTrackMetadataBlocks
	}
	if m.discardDeviceSupport {
		flags = flags | mountDiscardDeviceSupport
	}
	if m.disableDelayedAllocation {
		flags = flags | mountDisableDelayedAllocation
	}

	return flags
}

func parseMiscFlags(flags uint32) miscFlags {
	m := miscFlags{
		signedDirectoryHash:   flags&flagSignedDirectoryHash == flagSignedDirectoryHash,
		unsignedDirectoryHash: flags&flagUnsignedDirectoryHash == flagUnsignedDirectoryHash,
		developmentTest:       flags&flagTestDevCode == flagTestDevCode,
	}
	return m
}

func (m *miscFlags) toInt() uint32 {
	var flags uint32

	if m.signedDirectoryHash {
		flags = flags | flagSignedDirectoryHash
	}
	if m.unsignedDirectoryHash {
		flags = flags | flagUnsignedDirectoryHash
	}
	if m.developmentTest {
		flags = flags | flagTestDevCode
	}
	return flags
}
