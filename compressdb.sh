#!/bin/bash

set -eu

. jelly.conf

jar=$(pwd)/jar/Jelectrum.jar

cd $lobstack_path

rm -rf compress

for f in utxo_trie_map block_rescan_map special_block_store_map block_store_map special_object_map
#for f in utxo_trie_map
do
  db=$(echo $f|rev|cut -d "." -f 2-100|rev)
  echo "Working $db"
  java -Xmx2g -cp $jar lobstack.LobCompress . $db
  rm ${db}.????.data ${db}.root
  mv compress/${db}* .
  echo "----------------------------------------------------"
  
done


