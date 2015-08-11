package jelectrum;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.text.DecimalFormat;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class JelectrumDB
{
    protected Config conf;
    protected Map<Sha256Hash, SerializedTransaction> tx_map;
    //protected Map<String, HashSet<Sha256Hash> > address_to_tx_map;
    protected Map<Sha256Hash, StoredBlock> block_store_map;
    protected Map<String, StoredBlock> special_block_store_map;
    protected Map<Sha256Hash, SerializedBlock> block_map;
    //protected Map<Sha256Hash, HashSet<Sha256Hash> > tx_to_block_map;
    protected Map<Sha256Hash, String> block_rescan_map;
    protected Map<String, Object> special_object_map;
    protected Map<Integer, String> header_chunk_map;
    protected Map<String, UtxoTrieNode> utxo_trie_map;

    public JelectrumDB(Config conf)
    {
        this.conf = conf;

    }

    public void compact()
    {
    }


    public void commit()
    {
    }
    public void close()
    {
    }
    public void open()
    {

    }

    public abstract Map<Sha256Hash, StoredBlock> getBlockStoreMap();

    public abstract Map<String, StoredBlock> getSpecialBlockStoreMap();

    public abstract Map<Sha256Hash,SerializedTransaction> getTransactionMap();

    public abstract Map<Sha256Hash, SerializedBlock> getBlockMap();

    public abstract void addAddressToTxMap(String address, Sha256Hash hash);
    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash)
    {
      for(String a : addresses)
      {
        addAddressToTxMap(a, hash);
      }
    }
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      for(Map.Entry<String, Sha256Hash> me : lst)
      {
        addAddressToTxMap(me.getKey(), me.getValue());
      }
  
    }

    public abstract Set<Sha256Hash> getAddressToTxSet(String address);


    public abstract void addTxToBlockMap(Sha256Hash tx, Sha256Hash block);
    public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block)
    {
      for(Sha256Hash tx : txs)
      {
        addTxToBlockMap(tx, block);
      }
    }
    public abstract Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx);


    public abstract Map<Sha256Hash, String> getBlockRescanMap();

    public abstract Map<String, Object> getSpecialObjectMap();

    public abstract Map<Integer, String> getHeaderChunkMap();

    public Map<String, UtxoTrieNode> getUtxoTrieMap()
    {
      return utxo_trie_map;
    } 

}
