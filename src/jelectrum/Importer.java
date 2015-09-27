package jelectrum;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.bitcoinj.store.BlockStore;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.NetworkParameters;
import org.apache.commons.codec.binary.Hex;

import java.util.HashSet;

import org.bitcoinj.store.BlockStore;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.script.Script;
import java.util.Collection;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
import jelectrum.db.DBFace;

import org.junit.Assert;

public class Importer
{
    private LinkedBlockingQueue<Block> block_queue;
    private LinkedBlockingQueue<TransactionWork> tx_queue;

    private Jelectrum jelly;
    private TXUtil tx_util;
    private DBFace file_db;
    private MapBlockStore block_store;
    
    private LRUCache<Sha256Hash, Semaphore> in_progress;

    private NetworkParameters params;

    private AtomicInteger imported_blocks= new AtomicInteger(0);
    private AtomicInteger imported_transactions= new AtomicInteger(0);

    private int block_print_every=100;

    private volatile boolean run_rates=true;

    private LinkedList<StatusContext> save_thread_list;


    public Importer(NetworkParameters params, Jelectrum jelly, BlockStore block_store)
        throws org.bitcoinj.store.BlockStoreException
    {
        this.jelly = jelly;
        this.params = params;
        this.file_db = jelly.getDB();
        this.tx_util = new TXUtil(file_db, params);
        this.block_store = (MapBlockStore)block_store;

        Config config = jelly.getConfig();
        config.require("block_save_threads");
        config.require("transaction_save_threads");


        block_queue = new LinkedBlockingQueue<Block>(8);
        tx_queue = new LinkedBlockingQueue<TransactionWork>(512);

        in_progress = new LRUCache<Sha256Hash, Semaphore>(1024);

        save_thread_list = new LinkedList<StatusContext>();
        for(int i=0; i<config.getInt("block_save_threads"); i++)
        {
            BlockSaveThread t= new BlockSaveThread();
            save_thread_list.add(t);
            t.start();
        }

        for(int i=0; i<config.getInt("transaction_save_threads"); i++)
        {
            TransactionSaveThread t = new TransactionSaveThread();
            save_thread_list.add(t);
            t.start();
        }
        
        putInternal(params.getGenesisBlock());

        //checkConsistency();
        


    }

    public void start()
    {
        new RateThread("1-minute", 60000L).start();
        new RateThread("5-minute", 60000L * 5L).start();
        new RateThread("1-hour", 60000L * 60L).start();
    }


    public void checkConsistency()
        throws org.bitcoinj.store.BlockStoreException
    {
        StoredBlock head = block_store.getChainHead();

        StoredBlock curr_block = head;

        Sha256Hash genisis_hash = params.getGenesisBlock().getHash();
        int checked=0;

        while(true)
        {
            Sha256Hash curr_hash = curr_block.getHeader().getHash();

            if  (curr_block.getHeight() % 10000 == 0)
            {
                System.out.println("Block: " + curr_block.getHeight());
            }
            if (!file_db.getBlockMap().containsKey(curr_hash))
            {
                throw new RuntimeException("Missing block: " + curr_hash);
            }
            checked++;
            //if (checked > 20) return;
            
            if (curr_hash.equals(genisis_hash)) return;

            curr_block = curr_block.getPrev(block_store);

        }


    }

    public void saveBlock(Block b)
    {
        try
        {
            block_queue.put(b);
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }

    }



    public void saveTransaction(Transaction tx)
    {
      //Only bother saving lose transactions if we are otherwise up to date
      //otherwise this is just going to waste time importing transactions
      //that will either come with blocks or not at all later
      if (jelly.isUpToDate())
      {
        try
        {
            tx_queue.put(new TransactionWork(tx));
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }
      }

    }

    public class TransactionWork
    {

        public TransactionWork(Transaction tx)
        {
            this(tx, null, null);
        }
        public TransactionWork(Transaction tx, Semaphore sem, Sha256Hash block_hash)
        {
            this.tx = SerializedTransaction.scrubTransaction(jelly.getNetworkParameters(), tx);
            this.sem = sem;
            this.block_hash = block_hash;
        }
        Transaction tx;
        Semaphore sem;
        Sha256Hash block_hash;
    }

  public class BlockSaveThread extends Thread implements StatusContext
    {
    
        private volatile String status;
        private volatile long last_status_change;
        public BlockSaveThread()
        {
            setDaemon(true);
            setName("BlockSaveThread");
            setStatus("STARTUP");
        }
        public String getStatus()
        {
            return status;
        }

        public void setStatus(String new_status)
        {
            this.status = new_status;
            last_status_change = System.currentTimeMillis();
        }
 
        public long getLastStatusChangeTime()
        {
            return last_status_change;
        }
        public void run()
        {
            while(true)
            {
                
                try
                {
                    while (jelly.getSpaceLimited())
                    {
                      setStatus("SPACE_LIMIT_WAIT");
                      Thread.sleep(5000);
                    }
                    setStatus("BLK_QUEUE_WAIT");
                    Block blk = block_queue.take();
                    setStatus("BLK_WORK_START");

                    while(true)
                    {
                        try
                        {
                            putInternal(blk, this);
                            break;
                        }
                        catch(Throwable t)
                        {
                            System.out.println("Block "+blk.getHash()+" save failed.  Retrying");
                            jelly.getEventLog().log("Block "+blk.getHash()+" save failed.  Retrying");

                            t.printStackTrace();
                        }
                    }
                }
                catch(Throwable e)
                {
                    e.printStackTrace();
                }

            }
        }
    }


    public class TransactionSaveThread extends Thread implements StatusContext
    {
        private volatile String status;
        private volatile long last_status_change;
        public TransactionSaveThread()
        {
            setDaemon(true);
            setName("TransactionSaveThread");
            setStatus("STARTUP");
        }

        public String getStatus()
        {
            return status;
        }

        public void setStatus(String new_status)
        {
            this.status = new_status;
            last_status_change = System.currentTimeMillis();
        }
        public long getLastStatusChangeTime()
        {
            return last_status_change;
        }




        public void run()
        {
            while(true)
            {
                try
                {
                    setStatus("TX_QUEUE_WAIT");
                    TransactionWork tw = tx_queue.take();
                    setStatus("TX_WORK_START");

                    while(true)
                    {
                        try
                        {

                            putInternal(tw.tx, tw.block_hash, this);
                            break;
                        }
                        catch(Throwable e2)
                        {

                            System.out.println("Transaction "+tw.tx.getHash()+" save failed.  Retrying");
                            jelly.getEventLog().log("Transaction "+tw.tx.getHash()+" save failed.  Retrying");
                            e2.printStackTrace();
                            
                        }
                        finally
                        {
                        }
                    }



                    if (tw.sem != null)
                    {
                        tw.sem.release(1);
                    }

                }
                catch(Throwable e)
                {
                    e.printStackTrace();
                }
            }


        }

    }

    public void putTxOutSpents(Transaction tx)
    {
      LinkedList<String> tx_outs = new LinkedList<String>();

        for(TransactionInput in : tx.getInputs())
        {
            if (!in.isCoinBase())
            {
                TransactionOutPoint out = in.getOutpoint();
                String key = out.getHash().toString() + ":" + out.getIndex();
                //file_db.addTxOutSpentByMap(key, tx.getHash());
                tx_outs.add(key);

            }
        }
    }

    public void putInternal(Transaction tx, Sha256Hash block_hash)
    {
        putInternal(tx, block_hash, new NullStatusContext());
    }
    public void putInternal(Transaction tx, Sha256Hash block_hash, StatusContext ctx)
    {

        if (block_hash == null)
        {
          ctx.setStatus("TX_SERIALIZE");
          SerializedTransaction s_tx = new SerializedTransaction(tx);
          ctx.setStatus("TX_PUT");
          file_db.getTransactionMap().put(tx.getHash(), s_tx);
        }

        boolean confirmed = (block_hash != null);

        ctx.setStatus("TX_GET_ADDR");
        Collection<String> addrs = tx_util.getAllAddresses(tx, confirmed, null);

        Random rnd = new Random();

        ctx.setStatus("TX_SAVE_ADDRESS");
        file_db.addAddressesToTxMap(addrs, tx.getHash());

        imported_transactions.incrementAndGet();
        int h = -1;
        if (block_hash != null)
        {
            ctx.setStatus("TX_GET_HEIGHT");
            h = block_store.getHeight(block_hash);
        }

        ctx.setStatus("TX_NOTIFY");
        jelly.getElectrumNotifier().notifyNewTransaction(tx, addrs, h);
        ctx.setStatus("TX_DONE");

    }

    private void putInternal(Block block)
    {
        putInternal(block, new NullStatusContext());
    }
    private void putInternal(Block block, StatusContext ctx)
    {
        Sha256Hash hash = block.getHash();
        int h = block_store.getHeight(hash);
        long t1 = System.currentTimeMillis();

        ctx.setStatus("BLOCK_CHECK_EXIST");
        if (file_db.getBlockMap().containsKey(hash)) 
        {
            imported_blocks.incrementAndGet();
            return;
        }
        //Mark block as in progress


        Semaphore block_wait_sem;
        synchronized(in_progress)
        {
            block_wait_sem = in_progress.get(hash);
            if (block_wait_sem == null)
            {
                block_wait_sem = new Semaphore(0);
                in_progress.put(hash,block_wait_sem);
            }
        }


        
        int size=0;

        ctx.setStatus("BLOCK_TX_CACHE_INSERT");
        for(Transaction tx : block.getTransactions())
        {
          tx_util.saveTxCache(SerializedTransaction.scrubTransaction(params, tx));
        }

        ctx.setStatus("BLOCK_ADD_THINGS");
        file_db.addBlockThings(h, block);


        LinkedList<Sha256Hash> tx_list = new LinkedList<Sha256Hash>();
        HashMap<Sha256Hash, Collection<String>> addr_map = new HashMap<>();
        Collection<Map.Entry<String, Sha256Hash> > addrTxLst = new LinkedList<Map.Entry<String, Sha256Hash>>();
        Map<Sha256Hash, Transaction> block_tx_map = new HashMap<Sha256Hash, Transaction>();
        Map<Sha256Hash, SerializedTransaction> txs_map = new HashMap<Sha256Hash,SerializedTransaction>();

        for(Transaction tx : block.getTransactions())
        {
          block_tx_map.put(tx.getHash(), tx);
        }

        ctx.setStatus("BLOCK_GET_ADDRESSES");
        for(Transaction tx : block.getTransactions())
        {
          imported_transactions.incrementAndGet();
          Collection<String> addrs = tx_util.getAllAddresses(tx, true, block_tx_map);
          Assert.assertNotNull(addrs);
          //jelly.getEventLog().alarm("Saving addresses for tx: " + tx.getHash() + " - " + addrs);
          addr_map.put(tx.getHash(), addrs);

          for(String addr : addrs)
          {
            addrTxLst.add(new java.util.AbstractMap.SimpleEntry<String,Sha256Hash>(addr, tx.getHash()));
          }

          txs_map.put(tx.getHash(), new SerializedTransaction(tx));

          tx_list.add(tx.getHash());
          size++;
        }


        if (file_db.needsDetails())
        {


          ctx.setStatus("TX_SAVEALL");
          file_db.getTransactionMap().putAll(txs_map);

          ctx.setStatus("BLOCK_TX_MAP_ADD");
          file_db.addTxsToBlockMap(tx_list, hash);

          ctx.setStatus("ADDR_SAVEALL");
          file_db.addAddressesToTxMap(addrTxLst);

        }

        Assert.assertEquals(block.getTransactions().size(), addr_map.size());

        ctx.setStatus("TX_NOTIFY");
        for(Transaction tx : block.getTransactions())
        {
          Collection<String> addrs = addr_map.get(tx.getHash());
          
          //jelly.getEventLog().alarm("Notifying addresses for tx: " + tx.getHash() + " - " + addrs);
          Assert.assertNotNull(addrs);
          jelly.getElectrumNotifier().notifyNewTransaction(tx, addrs, h);
        }


        //Once all transactions are in, check for prev block in this store

        ctx.setStatus("BLOCK_WAIT_PREV");
        Sha256Hash prev_hash = block.getPrevBlockHash();
        
        waitForBlockStored(prev_hash);

        //System.out.println("Block " + hash + " " + Util.measureSerialization(new SerializedBlock(block)));

        ctx.setStatus("DB_COMMIT");
        file_db.commit();

        ctx.setStatus("BLOCK_SAVE");
        file_db.getBlockMap().put(hash, new SerializedBlock(block));

        block_wait_sem.release(1024);
        boolean wait_for_utxo = false;
        if (jelly.isUpToDate() && jelly.getUtxoTrieMgr().isUpToDate())
        {
          wait_for_utxo=true;
        }

        
        jelly.getUtxoTrieMgr().notifyBlock(wait_for_utxo);
        if (wait_for_utxo)
        {
          jelly.getEventLog().alarm("UTXO root hash: " + jelly.getUtxoTrieMgr().getRootHash());
        }
        jelly.getElectrumNotifier().notifyNewBlock(block);

        long t2 = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("0.000");
        double sec = (t2 - t1)/1000.0;


        if (h % block_print_every ==0)
        {
            jelly.getEventLog().alarm("Saved block: " + hash + " - " + h + " - " + size + " (" +df.format(sec) + " seconds)");
        }
        else
        {
          jelly.getEventLog().log("Saved block: " + hash + " - " + h + " - " + size + " (" +df.format(sec) + " seconds)");
        }
        imported_blocks.incrementAndGet();


    }

    private void waitForBlockStored(Sha256Hash hash)
    {
        if (hash.toString().equals("0000000000000000000000000000000000000000000000000000000000000000")) return;

        try
        {
            if( file_db.getBlockMap().containsKey(hash) ) return;
        }
        finally
        {

        }

        
        Semaphore block_wait_sem = null;
        synchronized(in_progress)
        {
            block_wait_sem = in_progress.get(hash);
            if (block_wait_sem == null)
            {
                block_wait_sem = new Semaphore(0);
                in_progress.put(hash,block_wait_sem);
            }
        }

        try
        {
            //System.out.println("Waiting for " + hash);
            block_wait_sem.acquire(1);
                
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }


    }
    public void setBlockPrintEvery(int n)
    {
        block_print_every = n;
    }

    public void disableRatePrinting()
    {
        run_rates=false;
    }
 

    public class RateThread extends Thread
    {
        private long delay;
        private String name;

        public RateThread(String name, long delay)
        {
            this.name = name;
            this.delay = delay;
            setDaemon(true);
            setName("Importer/RateThread/"+name);

        }

        public void run()
        {
            long blocks = 0;
            long transactions = 0;
            long last_run = System.currentTimeMillis();
            DecimalFormat df =new DecimalFormat("0.000");
            while(run_rates)
            {
                System.gc();
                try{Thread.sleep(delay);}catch(Exception e){}

                long now = System.currentTimeMillis();
                long blocks_now = imported_blocks.get();
                long transactions_now = imported_transactions.get();

                double sec = (now - last_run) / 1000.0;

                double block_rate = (blocks_now - blocks) / sec;
                double tx_rate = (transactions_now - transactions) / sec;
                if(run_rates)
                {
                    String rate_log = name + " Block rate: " + df.format(block_rate) + "/s   Transaction rate: " + df.format(tx_rate) + "/s" + "     txq:" + tx_queue.size() + " blkq:" + block_queue.size() ;

                    System.out.println(rate_log );
                    jelly.getEventLog().log(rate_log);
                    String status_report = getThreadStatusReport();
                    
                    jelly.getEventLog().alarm(status_report);
                    if (jelly.getPeerGroup().numConnectedPeers() == 0)
                    {
                      jelly.getEventLog().alarm("No connected peers - can't get new blocks or transactions");
                    }
                }

                blocks = blocks_now;
                transactions = transactions_now;
                last_run= now;
            }
        }
    }

    public interface StatusContext
    {
        public String getStatus();
        public void setStatus(String ns);
        public long getLastStatusChangeTime();
    }

    public class NullStatusContext implements StatusContext
    {
        public String getStatus(){return null;}
        public void setStatus(String ns){}
        public long getLastStatusChangeTime(){return System.currentTimeMillis();}

    }

    public String getThreadStatusReport()
    {
        TreeMap<String, Integer> status_map = new TreeMap<String, Integer>();
        for(StatusContext t : save_thread_list)
        {
            String status = t.getStatus();
            if (!status_map.containsKey(status))
            {
                status_map.put(status, 0);
            }
            status_map.put(status, status_map.get(status) + 1);
        }
        return status_map.toString();

    }


}
