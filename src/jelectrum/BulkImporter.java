package jelectrum;

import jelectrum.proto.Blockrepo;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.InflaterInputStream;
import com.google.protobuf.CodedInputStream;

import java.net.URL;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.AbstractMap.SimpleEntry;
import java.util.Scanner;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Sha256Hash;
import com.google.protobuf.ByteString;
import java.text.DecimalFormat;

public class BulkImporter
{
  private Jelectrum jelly;
  private TXUtil tx_util;

  private int BLOCKS_PER_CHUNK=100;

  //At 1mb per block, and 100 blocks per chunk, it is 100mb
  //per queue pack.  So memory can fill up fast.
  private static final int MAX_QUEUE=3;

  private LinkedBlockingQueue<Blockrepo.BitcoinBlockPack> pack_queue;

  int start_height;
  int bitcoind_height;

  double blocks_per_second_running_average=0.0;

  public BulkImporter(Jelectrum jelly)
    throws Exception
  {
    this.jelly = jelly;
    if (jelly.getConfig().isSet("bulk_import_blocks"))
    {
      BLOCKS_PER_CHUNK = jelly.getConfig().getInt("bulk_import_blocks");
    }
    this.tx_util = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());

    start_height = jelly.getBlockStore().getChainHead().getHeight() + 1;

    bitcoind_height = getMaxHeight();
    
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
      System.gc();

      /**
       * We don't have a running peer group yet, but that doesn't stop bitcoinj
       * from creating a confidence table and filling it with bullshit.
       * So clear it after each pack and we should be ok.
       */
      org.bitcoinj.core.Context.getOrCreate(jelly.getNetworkParameters()).getConfidenceTable().cleanTable();
      
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

        double blocks_to_go = bitcoind_height - pack.getStartHeight() - BLOCKS_PER_CHUNK;
        
        long t2 = System.nanoTime();
        double sec = (t2 - t1) / 1e9;
        double qsec = (queue_wait_time) / 1e9;
        double blks_sec = BLOCKS_PER_CHUNK / (sec + qsec);
        double txs_sec = tx_count / (sec + qsec);

        blocks_per_second_running_average = blocks_per_second_running_average * 0.95 + blks_sec * 0.05;

        double estimate_end_hours = (blocks_to_go / blocks_per_second_running_average) / 3600.0;
        DecimalFormat df = new DecimalFormat("0.000");
        DecimalFormat df1 = new DecimalFormat("0.0");
        jelly.getEventLog().alarm("Imported pack " + pack.getStartHeight() 
          + " - seconds (" + df.format(sec) + " processing) "
          +"(" + df.format(qsec) + " download wait) "
          +"(" + df.format(blks_sec) + " B/s) "
          +"(" + df.format(txs_sec) + " Objs/s) "
          +"(" + df1.format(estimate_end_hours) + " hours left)"
          );
        System.gc();

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
  
  private int getMaxHeight()
    throws Exception
  {
    String url = "https://ds73ipzb70zbz.cloudfront.net/blockchunk/"+BLOCKS_PER_CHUNK+"/max";
    URL u = new URL(url);
    Scanner scan = new Scanner(u.openStream());
    
    int h = scan.nextInt();
    scan.close();
    return h;

  }

  private long importPackThrows(Blockrepo.BitcoinBlockPack pack)
    throws Exception
  {
    LinkedList<Block> ordered_block_list = new LinkedList<>();

    if (jelly.getDB().needsDetails())
    {
      Map<Sha256Hash, SerializedTransaction> txs_map = new HashMap<>();
      Map<Sha256Hash, Transaction> tx_map = new HashMap<>();

      Map<Sha256Hash, SerializedBlock> block_map = new HashMap<>();
      Collection<Map.Entry<String, Sha256Hash> > addrTxLst = new LinkedList<>();
      Collection<Map.Entry<Sha256Hash, Sha256Hash> > blockTxLst = new LinkedList<>();
 
      jelly.getEventLog().alarm("Block Map...");

      for(Blockrepo.BitcoinBlock bblk : pack.getBlocksList())
      {
        SerializedBlock sblk = new SerializedBlock(bblk.getBlockData());
        Block blk = sblk.getBlock(jelly.getNetworkParameters());

        Sha256Hash block_hash = new Sha256Hash(bblk.getHash());

        jelly.getDB().addBlockThings(bblk.getHeight(), blk);      

        block_map.put(block_hash, sblk);
        ordered_block_list.add(blk);

      }

      jelly.getEventLog().alarm("TX Map...");
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


      //jelly.getEventLog().alarm("TX Save... " + txs_map.size());
      //This way the transactions will be availible if needed
      jelly.getDB().getTransactionMap().putAll(txs_map);

      
      //jelly.getEventLog().alarm("Get Addresses...");
      for(Transaction tx : tx_map.values())
      {
        Collection<String> addrs = tx_util.getAllAddresses(tx, true, tx_map);
        for(String addr : addrs)
        { 
          addrTxLst.add(new SimpleEntry<String,Sha256Hash>(addr, tx.getHash()));
        }      
      }

      
      //jelly.getEventLog().alarm("Save addresses... " + addrTxLst.size());
      // Add transaction mappings
      jelly.getDB().addAddressesToTxMap(addrTxLst);
      //jelly.getEventLog().alarm("Save tx block map... " + blockTxLst.size());
      jelly.getDB().addTxsToBlockMap(blockTxLst);

      
      //Save block headers
      //jelly.getEventLog().alarm("Save headers...");
      jelly.getBlockStore().putAll(ordered_block_list);

      

      //Save blocks themselves
      //jelly.getEventLog().alarm("Save blocks...");
      jelly.getDB().getBlockMap().putAll(block_map);



      jelly.getUtxoTrieMgr().start();
      jelly.getUtxoTrieMgr().notifyBlock(false);

      return tx_map.size() + block_map.size() + ordered_block_list.size() + blockTxLst.size() + addrTxLst.size();
    }
    else
    {
      Map<Sha256Hash, SerializedBlock> block_map = new HashMap<>();

      long tx_count = 0;
      TimeRecord time_rec = new TimeRecord();
      TimeRecord.setSharedRecord(time_rec);
      long t1;
      long t2;
      for(Blockrepo.BitcoinBlock bblk : pack.getBlocksList())
      {
        SerializedBlock sblk = new SerializedBlock(bblk.getBlockData());
        Block blk = sblk.getBlock(jelly.getNetworkParameters());
        Sha256Hash block_hash = new Sha256Hash(bblk.getHash());

        t1 = System.nanoTime();
        jelly.getDB().addBlockThings(bblk.getHeight(), blk);
        t2 = System.nanoTime();
        time_rec.addTime(t2-t1,"add_block_things");

        block_map.put(block_hash, sblk);
        ordered_block_list.add(blk);

        tx_count += blk.getTransactions().size();
      }

      //Save block headers
      t1 = System.nanoTime();
      //jelly.getEventLog().alarm("Save headers...");
      jelly.getBlockStore().putAll(ordered_block_list);
      time_rec.addTime(System.nanoTime() - t1, "save_headers");
      
      //jelly.getEventLog().alarm("Doing commit...");
      jelly.getDB().commit();

      //Save blocks themselves
      t1 = System.nanoTime();
      //jelly.getEventLog().alarm("Save blocks...");
      jelly.getDB().getBlockMap().putAll(block_map);
      time_rec.addTime(System.nanoTime() - t1, "save_blocks");


      jelly.getUtxoTrieMgr().start();
      jelly.getUtxoTrieMgr().notifyBlock(false);

      //time_rec.printReport(System.out);

      return tx_count;
    }
   

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
        String url = "https://ds73ipzb70zbz.cloudfront.net/blockchunk/" +BLOCKS_PER_CHUNK+"/" + pack_no;
        //String url = "https://s3-us-west-2.amazonaws.com/bitcoin-blocks/blockchunk/" +BLOCKS_PER_CHUNK+"/" + pack_no;
        
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
          CodedInputStream code_in = CodedInputStream.newInstance(de_in);
          code_in.setSizeLimit(256 * 1024 * 1024);

          Blockrepo.BitcoinBlockPack pack = Blockrepo.BitcoinBlockPack.parseFrom(code_in);

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
