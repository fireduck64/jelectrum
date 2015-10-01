
So your UTXO database is broken (you monster)

# Just Reset It

Add this to the jelly.conf and restart.  Make sure to remove it, as it will reset back to zero
height with every run if you leave it in.

```
utxo_reset=true
```

# If everything runs ok until you hit a certain block

Ok, this is slightly terrible but I have a tool that inspects a block and checks to see
if every output script resolves to the same public address bytes as the electrum-server.

Steps:
1. Clone the electrum-server repo.  The github page is https://github.com/spesmilo/electrum-server 
2. Copy check/check.py into 'src' into the cloned repo.
3. Add a config line pointing to the new location of that check script

```
utxo_check_tool=/home/clash/projects/bitcoin/electrum-server.git/src/check.py
```
4. Run the following command line:
```
java -cp jar/Jelectrum.jar jelectrum.UtxoTrieMgr jelly.conf 345014
```

Supstitute in your jelly.conf location and the block number you suspect a problem at.  (Generally the first block that gets a mismatch).
The jelly.conf has to have the utxo_check_tool configured and point to a valid jelectrum db with the block in question.


A positive no problems output looks like this:
```
{"id":"820196920","result":{"errors":"","relayfee":1.0E-5,"proxy":"","testnet":false,"difficulty":6.0813224039440346E10,"connections":26,"blocks":377030,"protocolversion":70010,"timeoffset":-1,"version":110000},"error":null}
2015-10-01T11:09:26.103 - Current Head: 000000000000000010c4570c1e93dbb744aa6be98707fc96cbbee82c3fee62fd - 377030 stepback 0
Block hash: 00000000000000000bcb84dd4916068bf519ea5d8f3c016535be5419ee1d815d
Inspecting 345014 - 00000000000000000bcb84dd4916068bf519ea5d8f3c016535be5419ee1d815d
TX Count: 861
Out Count: 1810
```

That means it checked 861 transactions containing 1810 output scripts and found nothing that electrum server resolves to a different public key.
If you get something that looks different, email fireduck@gmail.com.  Include the output from the command you just ran.


