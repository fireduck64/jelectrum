# History

This list details some of the fun with my search for the perfect key value store.

My experience is that many things can store data, but few seem to continue to perform
well after you store a few hundred million entries.

# Requirements

The jelectrum database requirements are simple.
There are two classes that need to be implemented for any DB implementation.

One is a simple key-value map.  It needs to support:
 * void put(ByteString key, ByteString value)
 * ByteString get(ByteString key)

This is used by most of the operations for blocks and various other things stored in the database.

The other is a keyed set of Sha256Hash hashes
 * void add(ByteString key, Sha256Hash hash)
 * Set<Sha256Hash> getSet(ByteString key)

This is used for mapping of addresses to transactions and transactions to blocks.

## RocksDB

Overview: Continued development of LevelDB by engineers at Facebook.

Downside: Used JNI so might have problems on other platforms or if the linux binaries doesn't match up correctly.

If you have a problem with strange error messages, try rebuilding the library on your system:
 * git clone https://github.com/facebook/rocksdb.git
 * Build java library - details on [RocksJava-Basics](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics)
        export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
        make rocksdbjava
 * copy java/target/rocksdbjni-4.2.0-linux64.jar (or whatever it gets named) into your jelectrum lib directory

## Slopbucket

Overview: A very simple memory mapped hash table implementation written by Fireduck to support
only the limited features listed above in requirements.

Downside: Probably will corrupt itself on unexpected system reboots
Downside: Required 64-bit system for memory mapped access

## MongoDB

Overview: A fast memory mapped database server

Downside: Occasionally loses data/corrupts self 
Downside: Required 64-bit system for memory mapped access

## Lobstack

Overview: A tree based data store written by Fireduck

Downside: Nothing is ever deleted so requires an IO costly compression to happen in the background
Downside: Complete insane

It will exit when space gets low (configurable with 'lobstack_minfree_gb').

You can observe lobstack pruning as it runs by watching lobstack.log


## LevelDB

Overview: A fast key value store.

Downside: Lacking a trustable JNI layer, jelectrum uses this via a C++ network server (levelnet) and a simple wire protocol.  This mostly works but sometimes things are slow with timeouts.
Downside: Occasionaly corrupts self: https://en.wikipedia.org/wiki/LevelDB#Bugs_and_Reliability

LevelDB seems cool and the C++ library seems good, so I've made a network layer to call into the C++ leveldb.

Make sure you are using a revent leveldb version.  1.17 and 1.18 seem to work well.

This is done because I've found things to be more reliable if the database is a separate process that doesn't
get restarted.  It is thus less likely to corrupt the datastore and be a problem.

Anyways, to run it, go into:
cd cpp
make

while true
do
./levelnet /var/ssd/leveldb
done

(replace that path with where you want the leveldb to live)

This will run th leveldb network server on port 8844.  There is absolutely no security or checking.
Firewall it.

Then you can run jelectrum with db_type=leveldb and copy the other settings from the sample file

Note: there are a few cases which cause levelnet to exit because that is the only sane thing to do.
In those cases, just run it again.  Jelectrum will be retying operation till they work and things should
resume.


## LMDB (JNI)

Overview: Memory mapped quick key value store

Downside: Performance seems to degrade rapidly as the database gets large

This uses JNI so if your system differs from mine, you might need to rebuild it.

Do a git clone of:
https://github.com/deephacks/lmdbjni

git clone https://github.com/deephacks/lmdbjni.git lmdbjni.git
cd lmdbjni.git
mvn install
cd lmdbjni-linux64
mvn install

Copy the resulting tar file into your jelectrum 'lib' dictory
Example: target/lmdbjni-linux64-0.4.5-SNAPSHOT.jar

## LMDB (Network)

Overview: To avoid possible issues with the JNI layer, uses the same wire protocol to talk to C++ lmdbnet just like LevelDB.

Downside: Performance seems to degrade rapidly as the database gets large

## Redis

Overview: A memory based key value server

Downside: Only works if you can fit your entire dataset in memory.  Which, lacking 100gb of ram, we can't.
Downside: Reads entire dataset on startup.

