package jelectrum;

import jelectrum.proto.Blockrepo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.InflaterInputStream;


import java.net.URL;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.AbstractMap.SimpleEntry;

import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.Sha256Hash;
import com.google.protobuf.ByteString;
import java.text.DecimalFormat;

public class BulkImporter
{
  private Jelectrum jelly;
  private TXUtil tx_util;

  private static final int BLOCKS_PER_CHUNK=100;

  //At 1mb per block, and 100 blocks per chunk, it is 100mb
  //per queue pack.  So memory can fill up fast.
  private static final int MAX_QUEUE=8;

  private LinkedBlockingQueue<Blockrepo.BitcoinBlockPack> pack_queue;

  int start_height;
  int bitcoind_height;

  public BulkImporter(Jelectrum jelly)
    throws Exception
  {
    this.jelly = jelly;
    this.tx_util = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());

    start_height = jelly.getBlockStore().getChainHead().getHeight();

    bitcoind_height = jelly.getBitcoinRPC().getBlockHeight();
    
    pack_queue = new LinkedBlockingQueue<>(MAX_QUEUE);


    int pack_count = getPackList().size();

    if (pack_count == 0) return;

    new DownloadThread().start();
    jelly.getEventLog().alarm("Starting bulk import of " + pack_count + " block packs");

    for(int pc = 0; pc<pack_count; pc++)
    {
      long t1 = System.nanoTime();
      Blockrepo.BitcoinBlockPack pack = pack_queue.take();
      long t2 = System.nanoTime();
      importPack(pack, t2 - t1);
    }





  }

  public List<Integer> getPackList()
  {
    int s = start_height - (start_height % BLOCKS_PER_CHUNK);

    int e = bitcoind_height - (bitcoind_height % BLOCKS_PER_CHUNK);

    LinkedList<Integer> lst = new LinkedList<>();

    for(int i=s; i<e; i+=BLOCKS_PER_CHUNK)
    {
      lst.add(i);
    }
    return lst;

  }

  
  private void importPack(Blockrepo.BitcoinBlockPack pack, long queue_wait_time)
  {
    long t1 = System.nanoTime();

    
    while(true)
    {
      try
      {
        long tx_count = importPackThrows(pack);
        
        long t2 = System.nanoTime();
        double sec = (t2 - t1) / 1e9;
        double qsec = (queue_wait_time) / 1e9;
        double blks_sec = BLOCKS_PER_CHUNK / (sec + qsec);
        double txs_sec = tx_count / (sec + qsec);
        DecimalFormat df = new DecimalFormat("0.000");
        jelly.getEventLog().alarm("Imported pack " + pack.getStartHeight() 
          + " - seconds (" + df.format(sec) + " processing) "
          +"(" + df.format(qsec) + " download wait) "
          +"(" + df.format(blks_sec) + " B/s) "
          +"(" + df.format(txs_sec) + " TX/s)"
          );

        return;
      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("Error in import of pack.  Will retry: " + t);
        t.printStackTrace();

      }
      try{Thread.sleep(10000);}catch(Throwable t){}
    }

  }

  private long importPackThrows(Blockrepo.BitcoinBlockPack pack)
    throws Exception
  {
    Map<Sha256Hash, SerializedTransaction> txs_map = new HashMap<>();
    Map<Sha256Hash, Transaction> tx_map = new HashMap<>();

    Map<Sha256Hash, SerializedBlock> block_map = new HashMap<>();
    LinkedList<Block> ordered_block_list = new LinkedList<>();

    Collection<Map.Entry<String, Sha256Hash> > addrTxLst = new LinkedList<>();
    Collection<Map.Entry<Sha256Hash, Sha256Hash> > blockTxLst = new LinkedList<>();

    for(Blockrepo.BitcoinBlock bblk : pack.getBlocksList())
    {
      SerializedBlock sblk = new SerializedBlock(bblk.getBlockData());
      Block blk = sblk.getBlock(jelly.getNetworkParameters());
      Sha256Hash block_hash = new Sha256Hash(bblk.getHash());

      block_map.put(block_hash, sblk);
      ordered_block_list.add(blk);

    }

    for(Block blk : ordered_block_list)
    {
      Sha256Hash blk_hash = blk.getHash();

      for(Transaction tx : blk.getTransactions())
      {
        tx_map.put(tx.getHash(), tx);
        txs_map.put(tx.getHash(), new SerializedTransaction(tx));

        blockTxLst.add(new SimpleEntry<Sha256Hash, Sha256Hash>(tx.getHash(), blk_hash));
      }
    }

    //This way the transactions will be availible if needed
    jelly.getDB().getTransactionMap().putAll(txs_map);

    for(Transaction tx : tx_map.values())
    {
      Collection<String> addrs = tx_util.getAllAddresses(tx, true, tx_map);
      for(String addr : addrs)
      { 
        addrTxLst.add(new SimpleEntry<String,Sha256Hash>(addr, tx.getHash()));
      }      
    }

    
    // Add transaction mappings
    jelly.getDB().addAddressesToTxMap(addrTxLst);
    jelly.getDB().addTxsToBlockMap(blockTxLst);

    
    //Save block headers
    jelly.getBlockStore().putAll(ordered_block_list);

    
    //Save blocks themselves
    jelly.getDB().getBlockMap().putAll(block_map);




    jelly.getUtxoTrieMgr().start();
    jelly.getUtxoTrieMgr().notifyBlock(false);

    return tx_map.size();
   

  }


  public class DownloadThread extends Thread
  {
    public DownloadThread()
    {
      setName("BulkImporter/DownloadThread");
      setDaemon(true);
    }

    public void run()
    {
      List<Integer> dl_lst = getPackList();
      for(int pack_no : dl_lst)
      {
        //String url = "https://ds73ipzb70zbz.cloudfront.net/blockchunk/" +BLOCKS_PER_CHUNK+"/" + pack_no;
        String url = "https://s3-us-west-2.amazonaws.com/bitcoin-blocks/blockchunk/" +BLOCKS_PER_CHUNK+"/" + pack_no;
        
        download(url);

      }

    }

    private void download(String url)
    {
      while(true)
      {
        try
        {
          URL u = new URL(url);

          InflaterInputStream de_in = new InflaterInputStream(u.openStream());

          Blockrepo.BitcoinBlockPack pack = Blockrepo.BitcoinBlockPack.parseFrom(de_in);

          de_in.close();

          pack_queue.put(pack);
          jelly.getEventLog().log("Download of " + url + " complete");
          
          return;
        }
        catch(Throwable t)
        {
          jelly.getEventLog().alarm("Error in download of " + url + ".  Will retry: " + t);
          t.printStackTrace();

        }
        try{sleep(10000);}catch(Throwable t){}



      }

    }


  }



}
