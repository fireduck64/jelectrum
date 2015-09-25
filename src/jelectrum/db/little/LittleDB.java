
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
import java.io.File;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
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
  private BitcoinRPC bitcoin_rpc;
  private TXUtil tx_util;

  public LittleDB(Config conf, EventLog log, BitcoinRPC bitcoin_rpc)
    throws Exception
  {
    super(conf);

    this.bitcoin_rpc = bitcoin_rpc;

    tx_util = new TXUtil(this, network_params);

    conf.require("little_path");

    File dir = new File(conf.get("little_path"));
    dir.mkdirs();

    File cake_dir = new File(dir, "bloom");
    cake_dir.mkdirs();

    cake = new BloomLayerCake(cake_dir, 750000);

    //block_map = new ObjectConversionMap<>(EXISTENCE, openMap("block_map"));
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
  }


  @Override
  public boolean needsDetails(){return false;}

  @Override
  public Set<Sha256Hash> getAddressToTxSet(String address)
  {
    return new HashSet<Sha256Hash>();

  }

  @Override
  public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
  {
    return new HashSet<Sha256Hash>();

  }

  @Override
  public SerializedTransaction getTransaction(Sha256Hash hash)
  {
    try
    {
      long t1 = System.nanoTime();
      String tx_hex = bitcoin_rpc.getTransaction(hash);
      TimeRecord.record(t1, "rpc_get_tx");
      if (tx_hex == null) return null;

      return new SerializedTransaction( Hex.decodeHex(tx_hex.toCharArray()));
    }
    catch(Exception e)
    {
      throw new RuntimeException(e);
    }

  }

}
