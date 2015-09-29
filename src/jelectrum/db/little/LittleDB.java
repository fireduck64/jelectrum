
package jelectrum.db.little;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.mongo.MongoDB;
import jelectrum.db.level.LevelDB;

import slopbucket.Slopbucket;
import jelectrum.Config;
import jelectrum.EventLog;
import jelectrum.BloomLayerCake;
import jelectrum.SerializedTransaction;
import jelectrum.BitcoinRPC;
import jelectrum.TXUtil;
import jelectrum.TimeRecord;
import jelectrum.BlockChainCache;
import jelectrum.SerializedBlock;
import jelectrum.TransactionSummary;
import jelectrum.BlockSummary;
import java.io.File;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import com.google.protobuf.ByteString;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.NetworkParameters;

import jelectrum.db.ObjectConversionMap;
import static jelectrum.db.ObjectConversionMap.ConversionMode.*;

import jelectrum.db.slopbucket.SlopbucketMap;
import org.apache.commons.codec.binary.Hex;

import org.junit.Assert;

public class LittleDB extends LevelDB
{
  private BloomLayerCake cake;
  private TXUtil tx_util;
  private NetworkParameters network_parameters;

  private HashMap<Sha256Hash, TransactionSummary> import_tx_summary_cache;

  public LittleDB(Config conf, EventLog log, NetworkParameters network_parameters)
    throws Exception
  {
    super(log, conf);

    this.network_parameters = network_parameters;

    import_tx_summary_cache = new HashMap<>();

    tx_util = new TXUtil(this, network_params);

    conf.require("little_path");

    File dir = new File(conf.get("little_path"));
    dir.mkdirs();

    File cake_dir = new File(dir, "bloom");
    cake_dir.mkdirs();

    cake = new BloomLayerCake(cake_dir, 750000);

    tx_map = null;

    if (conf.getBoolean("utxo_disable"))
    {
      utxo_trie_map = null;
    }

    /*{
      Sha256Hash h = new Sha256Hash("0000000000000000083fb1d19f7b2feb889a49f0dc25a5c60e5783f3cd4a734d");
      BlockSummary bs = getBlockSummaryMap().get(h);

      jelectrum.proto.Summary.BitcoinBlockSummary sum = bs.getProto();

      System.out.println("Block summary size protobuf: " + sum.toByteString().size());

      byte[] comp = lobstack.ZUtil.compress(sum.toByteString().toByteArray());
      System.out.println("Compressed protobuf: " + comp.length);


      {
        java.io.ByteArrayOutputStream b_out = new java.io.ByteArrayOutputStream();
        java.io.ObjectOutputStream o_steam = new java.io.ObjectOutputStream(b_out);
        o_steam.writeObject(bs);

        byte[] uncom = b_out.toByteArray();//Array();
        comp = lobstack.ZUtil.compress(uncom);
        System.out.println("Compressed object: " + comp.length);
      }

      getBlockSummaryMap().put(h, bs);
      System.out.println(bs.toString());
     
    }*/
  }


  @Override
  protected DBMapSet openMapSet(String name) throws Exception{return null;}


  @Override
  public void addBlockThings(int height, Block b)
  { 
    //System.out.println("Adding block " + height + " " + b.getHash());

    BlockSummary block_summary = new BlockSummary(height, b, tx_util, import_tx_summary_cache);

    getBlockSummaryMap().put(b.getHash(), block_summary);
    
    long t1=System.nanoTime();
    cake.addAddresses(height, block_summary.getAllAddresses());
    TimeRecord.record(t1, "cake_add_addresses");

  }

  @Override
  public synchronized void commit()
  {
    cake.flush();
    synchronized(import_tx_summary_cache)
    {
      import_tx_summary_cache.clear();
    }

  }


  @Override
  public boolean needsDetails(){return false;}

  @Override
  public Set<Sha256Hash> getAddressToTxSet(String address)
  {
    Set<Integer> heights = cake.getBlockHeightsForAddress(address);
    Set<Sha256Hash> out_list = new HashSet<Sha256Hash>();

    for(int height : heights)
    {
      Sha256Hash b = block_chain_cache.getBlockHashAtHeight(height);
      if (b != null)
      {
          BlockSummary bs = getBlockSummaryMap().get(b);

          out_list.addAll(bs.getMatchingTransactions(address));
      }
    }

    return out_list;


  }

  @Override
  public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
  {
    long t1=System.nanoTime();
    Set<Integer> heights = cake.getBlockHeightsForAddress(tx.toString());
    Set<Sha256Hash> blocks = new HashSet<Sha256Hash>();

    for(int height : heights)
    {
      //System.out.println("Height: " + height);
      Sha256Hash b = block_chain_cache.getBlockHashAtHeight(height);
      //if (b == null) System.out.println("Finding: " + tx + " no hash found for height: " + height + " head is: " + block_chain_cache.getHead());
      if (b != null)
      {
        Assert.assertNotNull(b);

        BlockSummary bs = getBlockSummaryMap().get(b);
        TransactionSummary ts = bs.getTxMap().get(tx);
        if (ts != null) blocks.add(b);

      }
    }
    TimeRecord.record(t1, "db_get_tx_to_block_map");
    

    return blocks;

  }

  @Override
  public TransactionSummary getTransactionSummary(Sha256Hash hash)
  {
    //System.out.println("Looking up tx: " + hash);
    long t1=System.nanoTime();
    synchronized(import_tx_summary_cache)
    {
      if (import_tx_summary_cache.containsKey(hash))
      {
        TimeRecord.record(t1, "db_get_txsummary_cached");
        return import_tx_summary_cache.get(hash);
      }
    }
    Set<Sha256Hash> block_list = getTxToBlockMap(hash);
    //System.out.println("Block list: " + block_list);

    for(Sha256Hash block_hash : block_list)
    {
      BlockSummary bs = getBlockSummaryMap().get(block_hash);
      TransactionSummary ts = bs.getTxMap().get(hash);

      TimeRecord.record(t1, "db_get_txsummary_loaded");
      return ts;

    }
    TimeRecord.record(t1, "db_get_txsummary_notfound");
    return null;

  }



  @Override
  public SerializedTransaction getTransaction(Sha256Hash hash)
  {
    System.out.println("Looking up tx: " + hash);
    long t1=System.nanoTime();
    Set<Sha256Hash> block_list = getTxToBlockMap(hash);
    //System.out.println("Get tx: " + hash + " - blocks: " + block_list);

    for(Sha256Hash block_hash : block_list)
    {
      SerializedBlock sb = getBlockMap().get(block_hash);
      if (sb != null)
      {
        Block b = sb.getBlock(network_parameters);

        for(Transaction tx : b.getTransactions())
        {
          if (tx.getHash().equals(hash))
          {
            TimeRecord.record(t1, "db_get_tx_found");
            return new SerializedTransaction(tx);
          }
        }

      }
    }
    TimeRecord.record(t1, "db_get_tx_not_found");
    return null;

  }

}
