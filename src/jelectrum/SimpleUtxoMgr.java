
package jelectrum;

import java.util.Collection;
import java.util.LinkedList;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Set;
import java.util.List;

import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.net.URL;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.core.Address;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;

import java.io.FileInputStream;
import java.util.Scanner;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.DataInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Hex;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import java.text.DecimalFormat;
import java.util.Random;
import org.junit.Assert;
import com.google.protobuf.ByteString;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import jelectrum.db.DBMapMutationSet;


import static org.bitcoinj.script.ScriptOpCodes.*;

/**
 * Blocks have to be loaded in order
 */
public class SimpleUtxoMgr implements UtxoSource
{

  // A key is 32 bytes of script hash, 32 bytes of transaction id and 4 bytes of output index
  public static final int ADDR_SPACE = 32 + 32 + 4;
  public static boolean DEBUG=false;

  private NetworkParameters params;
  private Jelectrum jelly;
  private TXUtil tx_util;

  private StatData get_block_stat=new StatData();
  private StatData add_block_stat=new StatData();
  private StatData get_hash_stat=new StatData();
 
  protected DBMapMutationSet db_map;

  protected Sha256Hash last_flush_block_hash;
  protected Sha256Hash last_added_block_hash;
      
  protected Object block_notify= new Object();
  protected Object block_done_notify = new Object();

  //Not to be trusted, only used for logging
  protected int block_height;

  protected volatile boolean caught_up=false;

  protected static PrintStream debug_out;

  private boolean enabled=true;

  private final Semaphore cache_sem = new Semaphore(0);
  private LRUCache<Sha256Hash, Boolean> root_hash_cache = new LRUCache<>(1024);

  public SimpleUtxoMgr(Jelectrum jelly)
    throws java.io.FileNotFoundException
  {
    this.jelly = jelly;
    this.params = jelly.getNetworkParameters();

    tx_util = jelly.getDB().getTXUtil(); 
    if (jelly.getConfig().getBoolean("utxo_disabled"))
    {
      enabled=false;
      return;
    }

    db_map = jelly.getDB().getUtxoSimpleMap();

    if (jelly.getConfig().isSet("utxo_reset") && jelly.getConfig().getBoolean("utxo_reset"))
    {
      jelly.getEventLog().alarm("UTXO reset");
      resetEverything();
    }

    if (DEBUG)
    {
      debug_out = new PrintStream(new FileOutputStream("utxo-debug.log"));
    }

  }

  public void setTxUtil(TXUtil tx_util)
  {
    this.tx_util = tx_util;
  }

  public boolean isUpToDate()
  {
    return caught_up;
  }

  private boolean started=false;

  public void start()
  {
    if (!enabled) return;

    if (started) return;
    started=true;

    new SimpleUtxoThread().start();

  }

  public void resetEverything()
  {
    last_flush_block_hash = params.getGenesisBlock().getHash();
    last_added_block_hash = params.getGenesisBlock().getHash();
    flush();

  }


  /**
   * Just public for testing, don't call
   */
  public void flush()
  {
    DecimalFormat df = new DecimalFormat("0.000");

    //get_block_stat.print("get_block", df);
    //add_block_stat.print("add_block", df);
    //get_hash_stat.print("get_hash", df);
    saveState(new UtxoStatus(last_added_block_hash));
    last_flush_block_hash = last_added_block_hash;
  }

  public void saveState(UtxoStatus status)
  {
    jelly.getDB().getSpecialObjectMap().put("utxo_trie_mgr_state", status);
  }

  public synchronized UtxoStatus getUtxoState()
  {
    if (!enabled) return null;

    try
    {
      Object o = jelly.getDB().getSpecialObjectMap().get("utxo_trie_mgr_state");
      if (o != null)
      {
        return (UtxoStatus)o;
      }
    }
    catch(Throwable t)
    {
      t.printStackTrace();
    }

    jelly.getEventLog().alarm("Problem loading UTXO status, starting fresh");
    resetEverything();
    return getUtxoState();
  }


  public synchronized void addBlock(Block b)
  {
    long t1 = System.nanoTime();

    Multimap<ByteString, ByteString> keys_to_add = HashMultimap.<ByteString,ByteString>create();
    Multimap<ByteString, ByteString> keys_to_remove = HashMultimap.<ByteString,ByteString>create();

    Map<Sha256Hash, Transaction> block_tx_map = new HashMap<>();
    for(Transaction tx : b.getTransactions())
    {
      block_tx_map.put(tx.getHash(), tx);
    }
    for(Transaction tx : b.getTransactions())
    {
      long t2 = System.nanoTime();
      addTransactionKeys(tx, keys_to_add, keys_to_remove, block_tx_map);
      TimeRecord.record(t2, "utxo_get_tx_keys");
    }

    {
    long t2 = System.nanoTime();
    db_map.addAll(keys_to_add.entries());
    TimeRecord.record(t2, "utxo_add_hash");
    }

    {
    long t2 = System.nanoTime();
    db_map.removeAll(keys_to_remove.entries());
    TimeRecord.record(t2, "utxo_remove_hash");
    }

    long t2 = System.nanoTime();
    last_added_block_hash = b.getHash();
    TimeRecord.record(t2, "utxo_gethash");
    TimeRecord.record(t1, "utxo_add_block");

  }


  // They see me rollin, they hating
  public synchronized void rollbackBlock(Block b)
  {
    LinkedList<Transaction> back_list = new LinkedList<Transaction>();

    for(Transaction tx : b.getTransactions())
    {
      back_list.addFirst(tx);
    }

    for(Transaction tx : back_list)
    {
      rollTransaction(tx);
    }

  }

  private void addTransactionKeys(Transaction tx, Multimap<ByteString, ByteString> keys_to_add, Multimap<ByteString, ByteString> keys_to_remove, Map<Sha256Hash, Transaction> block_tx_map)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      long t1 = System.nanoTime();
      ByteString key = getKeyForOutput(tx_out, idx);
      TimeRecord.record(t1, "utxo_get_key_for_output");
      if (key != null)
      {
        ByteString addr=key.substring(0,32);
        ByteString txinfo=key.substring(32);
        keys_to_add.put(addr,txinfo);
      }

      idx++;
    }

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        long t1 = System.nanoTime();
        ByteString key = getKeyForInput(tx_in, block_tx_map);
        TimeRecord.record(t1, "utxo_get_key_for_input");

        if (key != null)
        {
          ByteString addr=key.substring(0,32);
          ByteString txinfo=key.substring(32);
          keys_to_remove.put(addr,txinfo);
        }
      }
    }
  }
  

  private void rollTransaction(Transaction tx)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      ByteString key = getKeyForOutput(tx_out, idx);
      if (key != null)
      {
        ByteString addr=key.substring(0,32);
        ByteString txinfo=key.substring(32);
        db_map.remove(addr,txinfo);
      }
      idx++;
    }

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        ByteString key = getKeyForInput(tx_in, null);
        if (key != null)
        {
          ByteString addr=key.substring(0,32);
          ByteString txinfo=key.substring(32);
          db_map.add(addr,txinfo);
        }
      }
    }
  }

  @Override
  public synchronized Collection<TransactionOutPoint> getUnspentForScriptHash(ByteString prefix)
  { 
    Collection<ByteString> txinfo = db_map.getSet(prefix, 10000);

    LinkedList<TransactionOutPoint> outs = new LinkedList<TransactionOutPoint>();

      for(ByteString key : txinfo)
      {
        
        ByteString tx_data = key.substring(0,32);
        ByteBuffer bb = ByteBuffer.wrap(key.substring(32).toByteArray());
        bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int idx=bb.getInt();

        Sha256Hash tx_id = new Sha256Hash(tx_data.toByteArray());
        TransactionOutPoint o = new TransactionOutPoint(jelly.getNetworkParameters(), idx, tx_id);
        outs.add(o);
       
      }

    return outs;
  }


  @Override
  public void notifyBlock(boolean wait_for_it, Sha256Hash wait_for_block)
  {
    synchronized(block_notify)
    {
      block_notify.notifyAll();
    }

    if (wait_for_it)
    {
      long end_wait = System.currentTimeMillis() + 15000;
      while(end_wait > System.currentTimeMillis())
      {
        long wait_tm = end_wait - System.currentTimeMillis();
        if (wait_tm > 0)
        {
          try
          {
            synchronized(root_hash_cache)
            {
              if (root_hash_cache.containsKey(wait_for_block)) return;
            }
            synchronized(block_done_notify)
            {
              block_done_notify.wait(wait_tm);
            }
          }
          catch(Throwable t){}
        }
      }
    }
  
  }

  public static ByteString getKey(ByteString scriptHash, Sha256Hash tx_id, int idx)
  {
    try
    {
      ByteString.Output key_out = ByteString.newOutput(32+32+4);

      key_out.write(scriptHash.toByteArray());
      key_out.write(tx_id.getBytes());

      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      bb.putInt(idx);
      key_out.write(bb.array());
      return key_out.toByteString();
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }

  }
    public ByteString getKeyForInput(TransactionInput in, Map<Sha256Hash, Transaction> block_tx_map)
    {
      if (in.isCoinBase()) return null;

      ByteString scriptHash = tx_util.getScriptHashForInput(in, true, block_tx_map);
      return getKey(scriptHash, in.getOutpoint().getHash(), (int)in.getOutpoint().getIndex());

    }


    public ByteString getKeyForOutput(TransactionOutput out, int idx)
    {
      ByteString scriptHash = tx_util.getScriptHashForOutput(out);
      return getKey(scriptHash, out.getParentTransaction().getHash(), idx);
    }


  public class SimpleUtxoThread extends Thread
  {
    private BlockChainCache block_chain_cache;

    public SimpleUtxoThread()
    {
      setName("SimpleUtxoThread");
      setDaemon(true);

      block_chain_cache = jelly.getBlockChainCache();

    }

    public void run()
    {
      recover();

      while(true)
      {
        while(catchup()){}
        waitForBlocks();
      }

    }
    private void recover()
    {
      UtxoStatus status = getUtxoState();

      last_flush_block_hash = status.getBlockHash();
      last_added_block_hash = status.getBlockHash();

    }
    int added_since_flush = 0;
    long last_flush = 0;
    private boolean catchup()
    {
      while(!block_chain_cache.isBlockInMainChain(last_added_block_hash))
      {
        rollback();
      }

      int curr_height = jelly.getDB().getBlockStoreMap().get(last_added_block_hash).getHeight();
      int head_height = jelly.getDB().getBlockStoreMap().get(
        jelly.getBlockChainCache().getHead()).getHeight();

      boolean near_caught_up=caught_up;

      for(int i=curr_height+1; i<=head_height; i++)
      {
        while(jelly.getSpaceLimited())
        {
          try
          {
            Thread.sleep(5000);
          }
          catch(Throwable t){}
        }
       
        Sha256Hash block_hash = jelly.getBlockChainCache().getBlockHashAtHeight(i);
        long t1=System.currentTimeMillis();
        SerializedBlock sb = jelly.getDB().getBlock(block_hash);
        if (sb == null) 
        {
          try{Thread.sleep(250); return true;}catch(Throwable t){}
        }
        caught_up=false;
        Block b = sb.getBlock(params);
        long t2=System.currentTimeMillis();

        get_block_stat.addDataPoint(t2-t1);

        if (b.getPrevBlockHash().equals(last_added_block_hash))
        {
          t1=System.currentTimeMillis();
          addBlock(b);
          t2=System.currentTimeMillis();

          add_block_stat.addDataPoint(t2-t1);

          synchronized(root_hash_cache)
          {
            root_hash_cache.put(block_hash, true);
          }

          block_height=i;

          if ((near_caught_up) && (jelly.isUpToDate()))
          {
            jelly.getEventLog().alarm("UTXO added block " + i);
          }
          else
          {
            jelly.getEventLog().log("UTXO added block " + i);

          }


          added_since_flush++;

        }
        else
        {
          return true;
        }
        int flush_mod = 1000;
        //After the blocks get bigger, flush more often
        if (block_height > 220000) flush_mod = 100;
        if (block_height > 320000) flush_mod = 10;
        
        if (i % flush_mod == 0)
        {
          flush();
          added_since_flush=0;
          last_flush = System.currentTimeMillis();
        }
      
      }

      synchronized(block_done_notify)
      {
        block_done_notify.notifyAll();
      }

      if ((added_since_flush > 0) && (last_flush +15000L < System.currentTimeMillis()))
      {
        flush();
        added_since_flush=0;
        last_flush = System.currentTimeMillis();
        
      }

      return false;

    }

    private void rollback()
    {
      jelly.getEventLog().alarm("UTXO rolling back " + last_added_block_hash);

      Sha256Hash prev = jelly.getDB().getBlockStoreMap().get(last_added_block_hash).getHeader().getPrevBlockHash();

      last_flush_block_hash = prev;
      Block b = jelly.getDB().getBlock(last_added_block_hash).getBlock(jelly.getNetworkParameters());
      rollbackBlock(b);

      //Setting hashes such that it looks like we are doing prev -> last_added_block_hash
      //That way on recovery we will re-add the block and then roll back again
      flush();

      last_added_block_hash = prev;


    }
    private void waitForBlocks()
    {
      caught_up=true;


      synchronized(block_done_notify)
      {
        block_done_notify.notifyAll();
      }
      synchronized(block_notify)
      {
        try
        {
          block_notify.wait(10000);
        }
        catch(InterruptedException e){}
      }

    }

  }



}
