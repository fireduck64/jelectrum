jelectrum
=========

An Electrum Server written in Java

This is designed as replacement for the python electrum server:
https://github.com/spesmilo/electrum-server

Reasons to exist
----------------

1) The inital blockchain sync is multithreaded and a good bit faster than the python implementation
(2 - 3 days with a fast system and SSD)

2) It is good to have multiple implementations of things in general


DB Options
----------

Not sure what to use?  Use lmdb. 


LMDB
----

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


LevelDB
-------

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


DB MongoDB
----------

MongoDB: Install and setup mongodb.  A single node instance is just fine.  Run mongodb on SSD if possible.
On startup, jelectrum will create tables as needed.


PostgreSQL
----------

PostgreSQL: Install and setup postgresql.
On startup, jelectrum will create tables and indexes needed.  You'll need to create the database and a user and put it in the config.


Lobstack
--------
Lobstack: Copy the lobstack config entries and modify them.  

It will exit when space gets low (configurable with 'lobstack_minfree_gb').

You can observe lobstack pruning as it runs by watching lobstack.log



How to run
----------

Make your SSL cert:
./makekey.sh

Build:
ant jar

Config:
cp jelly.default.conf jelly.conf
edit jelly.conf as makes sense

Run:
java -Xmx8g -jar jar/Jelectrum.jar jelly.conf

My Instance
-----------

Feel free to connect to my instance which should be running the latest version.

```
b.1209k.com 50001 (tcp)
b.1209k.com 50002 (ssl)
h.1209k.com 50001 (tcp)
h.1209k.com 50002 (ssl)
```

What Doesn't Work
-----------------

1) HTTP/HTTPS.  I see no strong reason to support those over TCP and SSL+TCP.  I imagine it wouldn't be too hard
but I don't see the need.  If someone feels otherwise, let me know.

2) The following commands that clients don't seem to issue (yet):
```
blockchain.address.get_proof
blockchain.address.get_mempool
blockchain.utxo.get_address
```

(To check this list compare src/blockchain_processor.py from electrum-server to StratumConnection.java)

Monitoring
----------

If you are going to run it, monitor it!

I've made a monitoring tool that anyone to can use to monitor their electrum server (jelectrum or otherwise):

https://1209k.com/bitcoin-eye/ele.php





