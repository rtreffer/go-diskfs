# ext4
This file describes the layout on disk of ext4. It is a living document and probably will be deleted rather than committed to git.

The primary reference document is [here](https://ext4.wiki.kernel.org/index.php/Ext4_Disk_Layout#Overview), while helpful examples are [here](https://digital-forensics.sans.org/blog/2017/06/07/understanding-ext4-part-6-directories) and [here](https://medium.com/@metebalci/a-minimum-complete-tutorial-of-linux-ext4-file-system-53a5efac2c1f).

## Concepts

* Sector: a section of 512 bytes
* Block: a contiguous group of sectors. Block size usually is either 4K (4096 bytes) or 1K (1024 bytes), i.e. 8 sectors or 2 sectors. Block size minimum is 1KB (2 sectors), max is 64KB (128 sectors). Each block is associated with exactly one file. A file may contain more than one block - e.g. if a file is larger than the size of a single block - but each block belongs to exactly one file.
* inode: metadata about a file or directory. Each inode contains metadata about exactly one file. The number of inodes in a system is identical to the number of blocks for 32-bit, or far fewer for 64-bit.
* Block group: a contiguous group of blocks. Each block group is (8*block_size_in_bytes) blocks. So if block size is 4K, or 4096 bytes, then a block group is 8*4096 = 32,768 blocks, each of size 4096 bytes, for a block group of 128MB. If block size is 1K, a block group is 8192 blocks, or 8MB.
* 64-bit feature: ext4 filesystems normally uses 32-bit, which means the maximum blocks per filesystem is 2^32. If the 64-bit feature is enabled, then the maximum blocks per filesystem is 2^64.
* Superblock: A block that contains information about the entire filesystem. Exists in block group 0 and sometimes is backed up to other block groups. The superblock contains information about the filesystem as a whole: inode size, block size, last mount time, etc.
* Block Group Descriptor: Block Group Descriptors contain information about each block group: start block, end block, inodes, etc. One Descriptor per Group. But it is stored next to the Superblock (and backups), not with each Group.


### Block Group
Each block group is built in the following order:

1. Padding: 1024 bytes, only in Group 0, used for boot sector
2. Superblock: One block, only in Group 0 and any backups.
3. Group Descriptors: Many blocks, only in Group 0 and any backups.
4. Reserved GDT Blocks: Many blocks, reserved in case we need to expand to more Group Descriptors in the future. Only in Group 0 and any backups.
5. Data block bitmap: 1 block. One bit per block in the block group. Set to 1 if a data block is in use, 0 if not. In non-superblock groups (i.e. other than Group 0 and any backups), this is the first block.
6. inode bitmap: 1 block. One bit per inode in the block group. Set to 1 if an inode is in use, 0 if not.
7. inode table: many blocks. Calculated by `(inodes_per_group)*(size_of_inode)`. Remember that `inodes_per_group` = `blocks_per_group` = `8*block_size_in_bytes`. The original `size_of_inode` in ext2 was 128 bytes. In ext4 it uses 156 bytes, but is stored in 256 bytes of space, so `inode_size_in_bytes` = 256 bytes.
8. Data blocks: all of the rest of the blocks in the block group


## How to
In order to get to any particular file or directory in the ext4 filesystem, you need to "walk the tree".

### Walk the Tree
In order to get to any point, you need to walk the tree. For example, say you want to read the contents of directory `/usr/local/bin/`.

1. Find the inode of the root directory in the inode table. This **always** is inode 2.
2. Read inode of the root directory to get the data blocks that contain the contents of the root directory.
3. Read the directory entries in the data blocks to get the names of the files and directories in root. Continue until you find the one whose name matches the desired subdirectory. For example `usr`
4. Using the matched directory entry, get the inode number for that subdirectory.
5. Read inode of that subdirectory in the inode table to get the data blocks that contain the contents of that directory.
6. Repeat until you have read the data blocks for the desired entry.


### Read Directory Entries
To read directory entries

1. Walk the tree until you find the inode for the directory you want.
2. Read the data blocks pointed to by that inode
3. Interpret the data blocks.

The directory itself is just a single "file". It has an inode that indicates the file "length", which is the number of bytes that the listing takes up.

There are two types of directories: Classic and Hash Tree. Classic are just linear, unsorted, unordered lists of files. They work fine for shorter lists, but large directories can be slow to traverse if they grow too large. Once the contents of the directory "file" will be larger than a single block, ext4 switches it to a Hash Tree Directory Entry.

Which directory type it is - classical linear or hash tree - do not affect the inode, for which it is just a file, but the contents of the directory entry "file". You can tell if it is linear or hash tree by checking the inode flag `EXT4_INDEX_FL`. If it is set (i.e. `& 0x1000`), then it is a hash tree.

#### Classic Directory Entry
Each directory entry is at most 263 bytes long. They are arranged in sequential order in the file. The contents are:

* first four bytes are a `uint32` giving the inode number
* next 2 bytes give the length of the directory entry (max 263)
* next 1 byte gives the length of the file name (which could be calculated from the directory entry length...)
* next 1 byte gives type: unknown, file, directory, char device, block device, FIFO, socket, symlink
* next (up to 255) bytes contain chars with the file or directory name

The above is for the second version of ext4 directory entry (`ext4_dir_entry_2`). The slightly older version (`ext4_dir_entry`) is similar, except it does not give the file type, which in any case is in the inode. Instead it uses 2 bytes for the file name length.

#### Hash Tree Directory Entry
Entries in the block are structured as follows:

* `.` and `..` are the first two entries, and are classic `ext4_dir_entry_2`
* Look in byte `0x1c` to find the hash algorithm
* take the desired file/subdirectory name (just the `basename`) and hash it
* look in the root directory entry in the hashmap to find the relative block number. Note that the block number is relative to the block in the directory, not the filesystem or block group.
* Next step depends on the hash tree depth:
    * Depth = 0: read directory entry from the given block.
    * Depth > 0: use the block as another lookup table, repeating the steps above, until we come to the depth.
* Once we have the final leaf block given by the hash table, we just read the block sequentially; it will be full of classical directory entries linearly.

When reading the hashmap, it may not match precisely. Instead, it will fit within a range. The hashmap is sorted by `>=` to `<`. So if the table has entries as follows:

| Hash   | Block |
| -------|-------|
| 0      | 1     |
| 100    | 25    |
| 300    | 16    |

Then:

* all hash values from `0`-`99` will be in block `1`
* all hash values from `100-299` will be in block `25`
* all hash values from `300` to infinite will be in block `16`


### Create a Directory Entry
To create a directory, you need to go through the following steps:

1. "Walk the tree" to find the parent directory. E.g. if you are creating `/usr/local/foo`, then you need to walk the tree to get to the directory "file" for `/usr/local`. If the parent directory is just the root `/`, e.g. you are creating `/foo`, then you use the root directory, whose inode always is `2`.
2. Determine if the parent directory is classical linear or hash tree, by checking the flag `EXT4_INDEX_FL` in the parent directory's inode.
3. Create the appropriate entry.

#### Hash Tree

1. Calculate the hash of the new directory entry name
2. Determine which block in the parent directory "file" the new entry should live, based on the hash table.
3. Find the block.
4. Add a classical linear entry at the end of it.
5. Update the inode for the parent directory with the new file size.

If there is no room at the end of the block, you need to rebalance the hash tree. See below.

#### Classical Linear

1. Find the last block in the parent directory "file"
2. Add a classical linear directory entry at the end of it.
3. Update the inode for the parent directory with the new file size.

If this entry will cause the directory "file" to extend beyond a single block, convert to a hash tree. See below.

### Rebalance Hash Tree


### Convert Classical Linear to Hash Tree


### Read File Contents

### Create File

### Write File Contents
