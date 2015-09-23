package jelectrum;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;


import com.google.protobuf.ByteString;
import java.util.Map;
import java.util.HashSet;
import java.io.File;

/**
 * Crams all the known blocks into a bloom filter, mostly to test
 * with real data.  
 */
public class BloomBaker extends Thread
{
  private Jelectrum jelly;
  private BloomLayerCake cake;
  private TXUtil tx_util;

  public BloomBaker(Jelectrum jelly)
    throws Exception
  {
    this.jelly = jelly;
    tx_util = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());

    setName("BloomBaker");
    setDaemon(true);

    cake = new BloomLayerCake(new File("/var/ssd/clash/bloombaked"), 750000);


  }

  public void run()
  {
    while(true)
    {
      try
      {
        doUploadRun();

      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("BloomBaker error: " + t);
        t.printStackTrace();
      }
      try{ sleep(600000); } catch(Throwable t){}
    }
    

  }

  private void doUploadRun()
  {
    jelly.getEventLog().alarm("baker starting");
    int head_height = jelly.getElectrumNotifier().getHeadHeight();
    Map<String, Object> special_object_map = jelly.getDB().getSpecialObjectMap();

    for(int start=0; start<=head_height; start++)
    {
      Sha256Hash hash = jelly.getBlockChainCache().getBlockHashAtHeight(start);

      String key = "bloombaker/" + start;


      Sha256Hash db_hash = (Sha256Hash) special_object_map.get(key);

      if ((db_hash == null) || (!db_hash.equals(hash)))
      {
        SerializedBlock blk = jelly.getDB().getBlockMap().get(hash);
        Block b = blk.getBlock(jelly.getNetworkParameters());
        HashSet<String> addresses = new HashSet<String>();
        for(Transaction tx : b.getTransactions())
        {
          addresses.addAll(tx_util.getAllAddresses(tx, true, null));
        }

        cake.addAddresses(start, addresses);

        special_object_map.put(key, hash);
        jelly.getEventLog().log("BloomBaker saved " + start + " addresses: " + addresses.size() );

      }
    }

  }


}
