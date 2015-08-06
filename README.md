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

How to run
----------

DB:
MongoDB: Install and setup mongodb.  A single node instance is just fine.  Run mongodb on SSD if possible.
On startup, jelectrum will create tables as needed.

PostgreSQL: Install and setup postgresql.
On startup, jelectrum will create tables and indexes needed.  You'll need to create the database and a user and put it in the config.

Make your SSL cert:
./makekey.sh

Build:
ant jar

Config:
cp jelly.sample.conf jelly.conf
edit jelly.conf as makes sense

Run:
java -Xmx4g -Xss256k -jar jar/Jelectrum.jar jelly.conf

My Instance
-----------

Feel free to connect to my instance which should be running the latest version.

```
b.1209k.com 50001 (tcp)
b.1209k.com 50002 (ssl)
```

What Doesn't Work
-----------------

1) IRC for advertising server.

2) Whatever cleverness is being done for UXTO for Electrum 2.0.  I'll have to wait to see
how this shakes out and then implement here.

3) HTTP/HTTPS.  I see no strong reason to support those over TCP and SSL+TCP.  I imagine it wouldn't be too hard
but I don't see the need.  If someone feels otherwise, let me know.

4) The following commands that clients don't seem to issue (yet):
```
blockchain.address.listunspent
blockchain.address.get_balance
blockchain.address.get_proof
blockchain.address.get_mempool
blockchain.utxo.get_address
```

(To check this list compare src/blockchain_processor.py from electrum-server to StratumConnection.java)




