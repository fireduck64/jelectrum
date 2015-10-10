package jelectrum.db;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;
import java.text.DecimalFormat;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.AbstractMap.SimpleEntry;
import static jelectrum.db.ObjectConversionMap.ConversionMode.*;

import jelectrum.SerializedTransaction;
import jelectrum.SerializedBlock;
import jelectrum.UtxoTrieNode;
import jelectrum.Config;
import jelectrum.Util;
import jelectrum.TXUtil;
import jelectrum.TransactionSummary;
import jelectrum.BlockSummary;
import jelectrum.BlockChainCache;
import jelectrum.CacheMap;


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
    protected Map<Integer, Sha256Hash> height_map;
    protected Map<String, UtxoTrieNode> utxo_trie_map;
    protected Map<Sha256Hash, BlockSummary> block_summary_map;
    protected DBMapSet address_to_tx_map;
    protected DBMapSet tx_to_block_map;
    protected NetworkParameters network_params;
    protected BlockChainCache block_chain_cache;
    protected TXUtil tx_util;

    public DB(Config conf)
    {
      this.conf = conf;

      network_params = Util.getNetworkParameters(conf);
      tx_util = new TXUtil(this, network_params);
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
        block_store_map = new ObjectConversionMap<>(STOREDBLOCK, openMap("block_store_map"), network_params);
        special_block_store_map = new ObjectConversionMap<>(STOREDBLOCK, openMap("special_block_store_map"), network_params);
        block_map = new ObjectConversionMap<>(OBJECT, openMap("block_map"));
        block_rescan_map = new ObjectConversionMap<>(STRING, openMap("block_rescan_map"));
        special_object_map = new ObjectConversionMap<>(OBJECT, openMap("special_object_map"));
        header_chunk_map = new ObjectConversionMap<>(STRING, openMap("header_chunk_map"));
        height_map = new ObjectConversionMap<>(SHA256HASH, openMap("height_map"));
        utxo_trie_map = new ObjectConversionMap<>(UTXONODE, openMap("utxo_trie_map"));

        //these chazwogers are big an expensive to parse into memory
        //so keeping a small cache of them makes sense
        block_summary_map = new CacheMap<Sha256Hash,BlockSummary>(32,
          new ObjectConversionMap<Sha256Hash,BlockSummary>(OBJECT, openMap("block_summary_map")));

        address_to_tx_map = openMapSet("address_to_tx_map");
        tx_to_block_map = openMapSet("tx_to_block_map");

    }
    public TXUtil getTXUtil(){return tx_util;}

    protected abstract DBMap openMap(String name) throws Exception;
    protected abstract DBMapSet openMapSet(String name) throws Exception;


    public Map<Sha256Hash, StoredBlock> getBlockStoreMap(){ return block_store_map; }
    public Map<String, StoredBlock> getSpecialBlockStoreMap() { return special_block_store_map; }
    public Map<Sha256Hash, SerializedTransaction> getTransactionMap() { return tx_map; }
    public Map<Sha256Hash, SerializedBlock> getBlockMap(){ return block_map; }
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx) { return tx_to_block_map.getSet(tx.toString()); }
    public Map<Sha256Hash, String> getBlockRescanMap() { return block_rescan_map; }
    public Map<String, Object> getSpecialObjectMap() { return special_object_map; }
    public Map<Integer, String> getHeaderChunkMap() {return header_chunk_map; }
    public Map<Integer, Sha256Hash> getHeightMap() {return height_map; }
    public Map<String, UtxoTrieNode> getUtxoTrieMap() { return utxo_trie_map; } 
    public Map<Sha256Hash, BlockSummary> getBlockSummaryMap() { return block_summary_map; }

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

    public void addBlockThings(int height, Block blk){}

    public boolean needsDetails(){return true;}

    public SerializedTransaction getTransaction(Sha256Hash hash)
    {
      SerializedTransaction stx = getTransactionMap().get(hash);
      if (stx != null) return stx;

      //ok lets try to get it via block
      Set<Sha256Hash> block_hash_set = getTxToBlockMap(hash);
      if (block_hash_set == null) return null;

      for(Sha256Hash block_hash : block_hash_set)
      {
        SerializedBlock sb = getBlockMap().get(block_hash);
        if (sb != null)
        {
          Block b = sb.getBlock(network_params);

          for(Transaction tx : b.getTransactions())
          {
            if (tx.getHash().equals(hash))
            {
              return new SerializedTransaction(tx);
            }
          }

        }
 
      }

      return null;
    }

    public TransactionSummary getTransactionSummary(Sha256Hash hash)
    {
      return new TransactionSummary(getTransaction(hash).getTx(network_params), tx_util, null);
    }

    public void setBlockChainCache(BlockChainCache block_chain_cache)
    {
      this.block_chain_cache = block_chain_cache;
    }


}
