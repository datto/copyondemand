## What is it?

copy-on-demand copies a file from one place to another, while exposing a block device that allows data to be read and written to the target file during the copy.

This is useful in scenarios where the source file is high latency and/or limited bandwidth and it is critical that the file be usable during the copy operation (e.g. disk images living on a remote NFS server, and you'd like to run a virtual machine based on that image while the disk is copying). In the NFS scenario this also allows for offloading disk write operations to the target machines, rather than potentially overloading the NFS server.

Miscellaneous other features:
* Driver is aware of sparse source files, so does not copy unnecessary 0's
* Configurable background copy speed, with support for all copy-on-demand processes sharing the same bandwidth limit
* Copy operations are optionally resumable - even in the case of driver crashes

## How?

This project utilizes the "nbd" kernel module. This allows a user space application to service all the reads/writes of a block device (in the form of /dev/nbd[0-127]).

The block driver splits the source file into chunks (currently 4k), and for each chunk:

* Read
  * If the chunk is already on the target, pass through the read to the backing file on the target
  * If the chunk is not on the target, read the chunk from the source, serve the bytes back to the user, and queue the chunk to be flushed to disk
* Write
  * Writes are always sent to the target disk
    * [Book keeping is necessary to ensure writes that aren't aligned with our block size are reconciled, this is described in detail in the "Developer Docs" section]

Notice that all bytes on the source are read exactly once. And no writes are ever sent back to our (presumptively slow) source file. This reduces the load, and improves the performance, of a networked filesystem for i/o intensive workloads.

## Prerequisites

Load nbd + set the max number nbd devices (`/dev/nbd[0-127]`):
```
/sbin/modprobe nbd nbds_max=128
```

It's highly recommended that you use a linux kernel >4.4. However, there are mitigations in the code to allow copy-on-demand to run in 4.4.

## CLI Usage:

```
./copy-on-demand [-v] [-vv] /dev/nbdX [source file] [backing file]
```

This will causes /dev/nbdX to immediately appear as if it has the full content + size of [source file]. In the background the content will be transferred from [source file] to [backing file].

Any read/writes to /dev/nbdX will first pull the blocks from [source file] (if necessary), and then perform the requested read/write operation.

Upon completion of the background transfer, all read/writes become passthrough to the backing file.

## Building

Go 1.11+ is required for building.

## Licensing

This software is licensed under the Apache Software License, version 2.0, as described in the `LICENSE` file, with the notable exception of the modified vendored copy of [`buse-go`](https://github.com/samalba/buse-go), which is MIT licensed, as described in `LICENSE.buse`.

# Developer docs

## Basic flow

Fundamentally copy-on-demand just copies a file from one location to another. It is agnostic to where these files are - but assumes in many places that the source file is very slow/far away (i.e. over NFS) and that the target file is relatively fast/close by (i.e. a spinning hard disk attached to the machine we're running on). During this copy, the target file appears to be fully available through an nbd block device (/dev/nbdX), even though some or all of the bytes may only be on the source. Blocks that are only available on the source will be pulled "on demand" and served back to the requesting process.

There are 2 main threads: the "dynamic thread" - this thread serves reads/writes coming from the user, the disk write thread - this thread flushes blocks read from the source disk to the target disk. The dynamic thread is pooled, so many dynamic reads/writes can be happening at once, whereas the queue is always singly threaded.

It is guaranteed that no dynamic actions will affect overlapping blocks - this is so we don't have to worry at any layer about serializing reads/writes affecting the same blocks.

## Concepts

As a supplement to these definitions, most of these concepts are visualized [here](docs/technical-specs.pdf).

* Block
  * Source and target are divided into arbitrarily sized blocks (const.BlockSize)
* Block map
  * Stores the state of blocks
  * Blocks can be either "synced" or "unsynced"
  * Synced indicates that it is safe to read/write directly on the target
  * Unsynced indicates that the block only exists on the source
* Dirty block map
  * Stores blocks that have been partially written on the target, and the ranges of the block that have been written
* Range lock
  * Locks a range of blocks
  * Any thread reading or writing to the target disk is responsible for holding a range lock for any blocks it operates on
  * State updates of either the block map or dirty block map require a range lock for any blocks that may be updated
* Disk flush queue
  * Contains blocks that have been read from the source, but have not yet been flushed to the target disk

## Entry point(s)

### CLI

* main is in `./cmd/copyondemand.go`, main is responsible for:
  * Initializing the buse driver
  * Signal handling (^C)
  * (Optionally) Polling the health of the CoD driver to display to the user, via the `SampleRate` function
* Requests coming from user space are sent (through the buse driver) to `./file_backed_device.go` `ReadAt` and `WriteAt`
* Disk flushes + dirty block reconciliation are performed in `write_worker.go` `ProcessQueue`

### Use as a library
* copy-on-demand can also be used as a library rather than a pre-built CLI tool
* This is useful when the "source" isn't a traditional file (e.g. perhaps a file stored in chunks on an object store)
* A basic library example can be found in `./examples/`

## Read walkthrough

* Read request comes in from user
* Determine if all affected blocks are synced
  * If yes pass through read
  * return
* Read any blocks from source that appeared to be unsynced
* Obtain a range lock for any affected blocks
* If any blocks are still unsynced or dirty (i.e. they weren't fixed while we were locked)
  * Read the blocks from source
  * Reconcile any dirty blocks with their target state
  * Enqueue any blocks read from source to the disk flush queue
* Read any synced blocks
* Merge synced blocks, with blocks dynamically pulled from source
  * (In practice these "merge" scenarios should be rare, disk boots generally result in reading and writing to ranges that are either fully synced or fully unsynced)
* Return read bytes to caller

## Write walkthrough

* Write request comes in from user
* Obtain a range lock for any affected blocks
* For any blocks that are fully overwritten by this operation, convert the blocks to "synced" in the block map
* For any partially written blocks, calculate the range of the block that will be overwritten, and submit it to the dirty block map
* Enqueue any dirty blocks to the disk flush queue
* Write to target disk

## Disk flush queue walkthrough

* Dequeue item
* If the item contains data
  * Lock the range of affected blocks
  * If any blocks have become "synced" (i.e. by a subsequent overwrite), discard them
  * For any dirty blocks, read from the target disk => apply the writes in memory
  * Write remaining and updated blocks to target
* If item contains a dirty block id
  * Ensure the block hasn't become synced before the dequeue
  * Read block from source
  * Obtain a range lock for the block id
  * Read block from target
  * Reconcile block in memory
  * Flush to target disk
