jelectrum
=========

An Electrum Server written in Java

This is designed as replacement for the python electrum server:
https://github.com/spesmilo/electrum-server

Reasons to exist
----------------

1) The inital blockchain sync is multithreaded and a good bit faster than the python implementation

2) It is good to have multiple implementations of things in general


How to run
----------

DB:
Install and setup mangodb.  A single node instance is just fine.  Run mongodb on SSD if possible.

Make your SSL cert:
./makekey.sh

Build:
ant jar

Config:
cp jelly.sample.conf jelly.conf
edit jelly.conf as makes sense

Run:
java -Xmx4g -Xss256k -jar run/Jelectrum.jar jelly.conf


