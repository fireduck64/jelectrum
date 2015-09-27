
UPGRAYEDD
---------

1. Checkout this branch (git checkout bitcoinj-old-merge)
2. ant clean
3. ant jar
4. Run for a while, until messages "Rewritting key: ...." stop appearing
5. Make sure at least one new block comes in
6. Go back to master (git checkout master)
7. ant clean
8. ant jar
9. run.  There will be a few errors (like blockchain cache broken) and a complete UTXO rebuild should start.

I know this sucks, but there are some improvements to the UTXO so it should at least be faster and I'd rather
not put in strange hacks to make the upgrade more seemless.

After the UTXO rebuild is done, it should be good and on a newer bitcoinj.  


