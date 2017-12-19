package jelectrum.db;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.LinkedList;
import java.text.DecimalFormat;
import com.google.protobuf.ByteString;

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

import jelectrum.Config;
import jelectrum.Util;
import jelectrum.TXUtil;
import jelectrum.BlockChainCache;
import jelectrum.CacheMap;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import jelectrum.DaemonThreadFactory;


public abstract class DB implements DBFace
{
    protected Config conf;
    protected Map<Sha256Hash, StoredBlock> block_store_map;
    protected Map<String, StoredBlock> special_block_store_map;
    protected Map<Sha256Hash, String> block_saved_map;
    protected Map<String, Object> special_object_map;
    protected Map<Integer, String> header_chunk_map;
    protected Map<Integer, Sha256Hash> height_map;
    protected DBMapMutationSet utxo_simple_map;
    protected DBMapMutationSet pubkey_to_tx_map;
    protected NetworkParameters network_params;
    protected BlockChainCache block_chain_cache;
    protected TXUtil tx_util;

    protected int max_set_return_count=100000;

    protected Executor exec;

    protected RawBitcoinDataSource rawSource;


    public DB(Config conf)
    {
      this.conf = conf;

      network_params = Util.getNetworkParameters(conf);
      tx_util = new TXUtil(this, network_params);

      Runtime.getRuntime().addShutdownHook(new DBShutdownThread());
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
        //tx_map = new ObjectConversionMap<>(SERIALIZEDTRANSACTION, openMap("tx_map"));
        block_store_map = new ObjectConversionMap<>(STOREDBLOCK, openMap("block_store_map"), network_params);
        special_block_store_map = new ObjectConversionMap<>(STOREDBLOCK, openMap("special_block_store_map"), network_params);
        //block_map = new ObjectConversionMap<>(OBJECT, openMap("block_map"));
        block_saved_map = new ObjectConversionMap<>(STRING, openMap("block_saved_map"));
        special_object_map = new ObjectConversionMap<>(OBJECT, openMap("special_object_map"));
        header_chunk_map = new ObjectConversionMap<>(STRING, openMap("header_chunk_map"));
        height_map = new ObjectConversionMap<>(SHA256HASH, openMap("height_map"));
        //utxo_simple_map = new ObjectConversionMap<>(STRING, openMap("utxo_simple_map"));


        //address_to_tx_map = openMapSet("address_to_tx_map");
        //tx_to_block_map = openMapSet("tx_to_block_map");

        pubkey_to_tx_map = openMutationMapSet("ptx");
        utxo_simple_map = openMutationMapSet("us");
    }
    public TXUtil getTXUtil(){return tx_util;}

    protected abstract DBMap openMap(String name) throws Exception;
    protected abstract DBMapSet openMapSet(String name) throws Exception;
    protected DBMapMutationSet openMutationMapSet(String name) throws Exception
    {
      throw new Exception("DBMapMutationSet not supported");
    }


    public Map<Sha256Hash, StoredBlock> getBlockStoreMap(){ return block_store_map; }
    public Map<String, StoredBlock> getSpecialBlockStoreMap() { return special_block_store_map; }
    public Map<Sha256Hash, String> getBlockSavedMap() { return block_saved_map; }
    public Map<String, Object> getSpecialObjectMap() { return special_object_map; }
    public Map<Integer, String> getHeaderChunkMap() {return header_chunk_map; }
    public Map<Integer, Sha256Hash> getHeightMap() {return height_map; }
    public DBMapMutationSet getUtxoSimpleMap() {return utxo_simple_map; }

    public void setRawBitcoinDataSource(RawBitcoinDataSource rawSource)
    {
      this.rawSource = rawSource;
    }

    public void addScriptHashToTxMap(Collection<ByteString> publicKeys, Sha256Hash hash)
    {
      LinkedList<Map.Entry<ByteString, ByteString>> lst = new LinkedList<>();
      for(ByteString a : publicKeys)
      {
        lst.add(new SimpleEntry<ByteString, ByteString>(a, ByteString.copyFrom(hash.getBytes())));
      }
      pubkey_to_tx_map.addAll(lst);

    }

    public void addScriptHashToTxMap(Collection<Map.Entry<ByteString, Sha256Hash> > lst)
    {
      LinkedList<Map.Entry<ByteString, ByteString>> out = new LinkedList<>();
      for(Map.Entry<ByteString, Sha256Hash> me : lst)
      {
        out.add(new SimpleEntry<ByteString, ByteString>(me.getKey(), ByteString.copyFrom(me.getValue().getBytes())));
    
      }
      pubkey_to_tx_map.addAll(out);
    }

    public Set<Sha256Hash> getScriptHashToTxSet(ByteString publicKey)
    {
      Set<ByteString> bs = pubkey_to_tx_map.getSet(publicKey, max_set_return_count);

      HashSet<Sha256Hash> ret = new HashSet<>();
      for(ByteString b : bs)
      {
        ret.add(new Sha256Hash(b.toByteArray()));
      }
      return ret;
    }


    /*public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
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
    } */

    public void addBlockThings(int height, Block blk){}

    public boolean needsDetails(){return true;}

    public SerializedTransaction getTransaction(Sha256Hash hash)
    {
      return rawSource.getTransaction(hash);

      /*SerializedTransaction stx = getTransactionMap().get(hash);
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
              return new SerializedTransaction(tx, b.getTime().getTime());
            }
          }

        }
 
      }

      return null;*/
    }
    public SerializedBlock getBlock(Sha256Hash hash)
    {
      return rawSource.getBlock(hash);
    }

    public void setBlockChainCache(BlockChainCache block_chain_cache)
    {
      this.block_chain_cache = block_chain_cache;
    }


    /** Override if the database needs to do soemthing on shutdown */
    protected void dbShutdownHandler() throws Exception
    {

    }

  protected synchronized Executor getExec()
  {
    if (exec == null)
    {
      exec = new ThreadPoolExecutor(
        32,
        32,
        2, TimeUnit.DAYS,
        new LinkedBlockingQueue<Runnable>(),
        new DaemonThreadFactory());
    }
    return exec;

  }

  public class DBShutdownThread extends Thread
  {
    public DBShutdownThread()
    {
      setName("DBShutdownHandler");
    }

    public void run()
    {
      try
      {
        dbShutdownHandler();
      }
      catch(Throwable t)
      {
        System.out.println("Exception in DB shutdown: " + t);
        t.printStackTrace();
      }
    }

  }
}
