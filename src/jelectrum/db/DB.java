package jelectrum.db;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;
import java.text.DecimalFormat;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.AbstractMap.SimpleEntry;
import static jelectrum.db.ObjectConversionMap.ConversionMode.*;

import jelectrum.SerializedTransaction;
import jelectrum.SerializedBlock;
import jelectrum.UtxoTrieNode;
import jelectrum.Config;


public abstract class DB implements DBFace
{
    protected Config conf;
    protected Map<Sha256Hash, SerializedTransaction> tx_map;
    protected Map<Sha256Hash, StoredBlock> block_store_map;
    protected Map<String, StoredBlock> special_block_store_map;
    protected Map<Sha256Hash, SerializedBlock> block_map;
    protected Map<Sha256Hash, String> block_rescan_map;
    protected Map<String, Object> special_object_map;
    protected Map<Integer, String> header_chunk_map;
    protected Map<String, UtxoTrieNode> utxo_trie_map;
    protected DBMapSet address_to_tx_map;
    protected DBMapSet tx_to_block_map;

    public DB(Config conf)
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
      throws Exception
    {
        tx_map = new ObjectConversionMap<>(SERIALIZEDTRANSACTION, openMap("tx_map"));
        block_store_map = new ObjectConversionMap<>(OBJECT, openMap("block_store_map"));
        special_block_store_map = new ObjectConversionMap<>(OBJECT, openMap("special_block_store_map"));
        block_map = new ObjectConversionMap<>(OBJECT, openMap("block_map"));
        block_rescan_map = new ObjectConversionMap<>(STRING, openMap("block_rescan_map"));
        special_object_map = new ObjectConversionMap<>(OBJECT, openMap("special_object_map"));
        header_chunk_map = new ObjectConversionMap<>(STRING, openMap("header_chunk_map"));
        utxo_trie_map = new ObjectConversionMap<>(OBJECT, openMap("utxo_trie_map"));

        address_to_tx_map = openMapSet("address_to_tx_map");
        tx_to_block_map = openMapSet("tx_to_block_map");

    }

    protected abstract DBMap openMap(String name) throws Exception;
    protected abstract DBMapSet openMapSet(String name) throws Exception;


    public Map<Sha256Hash, StoredBlock> getBlockStoreMap(){ return block_store_map; }
    public Map<String, StoredBlock> getSpecialBlockStoreMap() { return special_block_store_map; }
    public Map<Sha256Hash,SerializedTransaction> getTransactionMap() { return tx_map; }
    public Map<Sha256Hash, SerializedBlock> getBlockMap(){ return block_map; }
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx) { return tx_to_block_map.getSet(tx.toString()); }
    public Map<Sha256Hash, String> getBlockRescanMap() { return block_rescan_map; }
    public Map<String, Object> getSpecialObjectMap() { return special_object_map; }
    public Map<Integer, String> getHeaderChunkMap() {return header_chunk_map; }
    public Map<String, UtxoTrieNode> getUtxoTrieMap() { return utxo_trie_map; } 



    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash)
    {
      LinkedList<Map.Entry<String, Sha256Hash>> lst = new LinkedList<>();

      for(String a : addresses)
      {
        lst.add(new SimpleEntry<String, Sha256Hash>(a, hash));
      }

      address_to_tx_map.addAll(lst);
    }

    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      address_to_tx_map.addAll(lst);
  
    }

    public Set<Sha256Hash> getAddressToTxSet(String address)
    {
      return address_to_tx_map.getSet(address);
    }


    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
    {
      tx_to_block_map.add(tx.toString(), block);
    }


    public final void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block)
    {
      LinkedList<Map.Entry<String, Sha256Hash>> lst = new LinkedList<>();
      for(Sha256Hash tx : txs)
      {
        lst.add(new SimpleEntry<String, Sha256Hash>(tx.toString(), block));
      }
      tx_to_block_map.addAll(lst);
    }

    public void addTxsToBlockMap(Collection<Map.Entry<Sha256Hash, Sha256Hash> > lst)
    {
      LinkedList<Map.Entry<String, Sha256Hash>> olst = new LinkedList<>();
      for(Map.Entry<Sha256Hash, Sha256Hash> me : lst)
      {
        olst.add(new SimpleEntry<String, Sha256Hash>(me.getKey().toString(), me.getValue()));

      }
      tx_to_block_map.addAll(olst);
    } 

}
