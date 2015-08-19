package jelectrum;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedList;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;

import jelectrum.LevelDBMap.ConversionMode;


public class JelectrumDBLevelDB extends JelectrumDB
{
    private Config conf;
    protected LevelDBMapSet address_to_tx_map;
    protected LevelDBMapSet tx_to_block_map;
    protected LevelNetClient client;

    public JelectrumDBLevelDB(Config config)
        throws Exception
    {
        super(config);

        this.conf = config;
        client = new LevelNetClient(config);

        open();


   }

    public synchronized void open()
    {

        try
        {

            tx_map = new LevelDBMap<Sha256Hash, SerializedTransaction>(client, "tx_map", ConversionMode.SERIALIZEDTRANSACTION);
            block_store_map = new CacheMap<Sha256Hash, StoredBlock>(25000,new LevelDBMap<Sha256Hash, StoredBlock>(client,"block_store_map",ConversionMode.OBJECT));
            special_block_store_map = new LevelDBMap<String, StoredBlock>(client, "special_block_store_map",ConversionMode.OBJECT);
            block_map = new CacheMap<Sha256Hash, SerializedBlock>(240,new LevelDBMap<Sha256Hash, SerializedBlock>(client,"block_map",ConversionMode.OBJECT));
            block_rescan_map = new LevelDBMap<Sha256Hash, String>(client, "block_rescan_map",ConversionMode.STRING);
            special_object_map = new LevelDBMap<String, Object>(client, "special_object_map",ConversionMode.OBJECT);
            header_chunk_map = new CacheMap<Integer, String>(200, new LevelDBMap<Integer, String>(client,"header_chunk_map",ConversionMode.STRING));
            utxo_trie_map = new CacheMap<String, UtxoTrieNode>(1000000, new LevelDBMap<String, UtxoTrieNode>(client, "utxo_trie_map",ConversionMode.OBJECT));

            address_to_tx_map = new LevelDBMapSet(client,"address_to_tx_map");
            tx_to_block_map = new LevelDBMapSet(client,"tx_to_block_map");

        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }


    }


    public synchronized Map<Sha256Hash, StoredBlock> getBlockStoreMap()
    {   
        return block_store_map;
    }

    public synchronized Map<String, StoredBlock> getSpecialBlockStoreMap()
    {   
        return special_block_store_map;
    }

    public synchronized Map<Sha256Hash,SerializedTransaction> getTransactionMap()
    {   
        return tx_map;
    }
    public synchronized Map<Sha256Hash, SerializedBlock> getBlockMap()
    {   
        return block_map;
    }


    public void addAddressToTxMap(String address, Sha256Hash hash)
    {
      address_to_tx_map.put(address, hash);
    }
    @Override
    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash)
    {
      LinkedList<Map.Entry<String, Sha256Hash> > lst = new LinkedList<Map.Entry<String, Sha256Hash> >();
      for(String a : addresses)
      {
        lst.add(new java.util.AbstractMap.SimpleEntry<String,Sha256Hash>(a, hash));
      }
      addAddressesToTxMap(lst);
      
    }
    @Override
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      address_to_tx_map.putList(lst);
    }
    public Set<Sha256Hash> getAddressToTxSet(String address)
    {
        return address_to_tx_map.getSet(address);
    }

   
  
    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
    {
      tx_to_block_map.put(tx.toString(), block);
    }
    @Override
    public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block)
    {

      LinkedList<Map.Entry<String, Sha256Hash> > lst = new LinkedList<Map.Entry<String, Sha256Hash> >();
      for(Sha256Hash a : txs)
      {
        lst.add(new java.util.AbstractMap.SimpleEntry<String,Sha256Hash>(a.toString(), block));
      }
 
      tx_to_block_map.putList(lst);

    }
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
    {
        return tx_to_block_map.getSet(tx.toString());
    }

    public synchronized Map<Sha256Hash, String> getBlockRescanMap()
    {
        return block_rescan_map;
    }

    public synchronized Map<String, Object> getSpecialObjectMap()
    {
        return special_object_map;
    }

    public synchronized Map<Integer, String> getHeaderChunkMap()
    {
        return header_chunk_map;
    }

}
