
package jelectrum.db.little;
import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.mongo.MongoDB;

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

public class LittleDB extends MongoDB
{
  private BloomLayerCake cake;
  private TXUtil tx_util;
  private NetworkParameters network_parameters;

  private HashMap<Sha256Hash, Transaction> import_tx_cache;

  public LittleDB(Config conf, EventLog log, NetworkParameters network_parameters)
    throws Exception
  {
    super(conf);

    this.network_parameters = network_parameters;

    import_tx_cache = new HashMap<>();

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
  }


  @Override
  protected DBMapSet openMapSet(String name) throws Exception{return null;}


  @Override
  public void addBlockThings(int height, Block b)
  { 
    for(Transaction tx : b.getTransactions())
    { 
      import_tx_cache.put(tx.getHash(), tx);
    }
    HashSet<String> addresses = new HashSet<String>();
    for(Transaction tx : b.getTransactions())
    { 
      long t1=System.nanoTime();
      addresses.addAll(tx_util.getAllAddresses(tx, true, null));
      TimeRecord.record(t1, "get_all_addresses");

      addresses.add(tx.getHash().toString());
    }

    long t1=System.nanoTime();
    cake.addAddresses(height, addresses);
    TimeRecord.record(t1, "cake_add_addresses");

  }

  @Override
  public void commit()
  {
    cake.flush();
    import_tx_cache.clear();
  }


  @Override
  public boolean needsDetails(){return false;}

  @Override
  public Set<Sha256Hash> getAddressToTxSet(String address)
  {
    Set<Integer> heights = cake.getBlockHeightsForAddress(address);
    Set<Sha256Hash> blocks = new HashSet<Sha256Hash>();

    for(int height : heights)
    {
      Sha256Hash b = block_chain_cache.getBlockHashAtHeight(height);
      if (b != null)
      {
        blocks.add(b);
      }
    }

    return blocks;


  }

  @Override
  public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
  {
    Set<Integer> heights = cake.getBlockHeightsForAddress(tx.toString());
    Set<Sha256Hash> blocks = new HashSet<Sha256Hash>();

    for(int height : heights)
    {
      Sha256Hash b = block_chain_cache.getBlockHashAtHeight(height);
      if (b != null)
      {
        blocks.add(b);
      }
    }

    return blocks;

  }

  @Override
  public SerializedTransaction getTransaction(Sha256Hash hash)
  {
    if (import_tx_cache.containsKey(hash))
    {
      return new SerializedTransaction(import_tx_cache.get(hash));
    }
    long t1=System.nanoTime();
    Set<Sha256Hash> block_list = getTxToBlockMap(hash);

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
            TimeRecord.record(t1, "get_tx_found");
            return new SerializedTransaction(tx);
          }
        }

      }
    }
    TimeRecord.record(t1, "get_tx_not_found");
    return null;

  }

}
