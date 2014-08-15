package jelectrum;


import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.NetworkParameters;
import org.apache.commons.codec.binary.Hex;

import java.util.HashSet;

import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.core.TransactionOutPoint;
import com.google.bitcoin.core.Address;
import com.google.bitcoin.core.ScriptException;
import com.google.bitcoin.script.Script;
import java.util.Collection;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.Random;
import java.util.LinkedList;
import java.util.TreeMap;

public class Importer
{
    private LinkedBlockingQueue<Block> block_queue;
    private LinkedBlockingQueue<TransactionWork> tx_queue;

    private Jelectrum jelly;
    private JelectrumDB file_db;
    private MapBlockStore block_store;
    
    //private LRUCache<String, Object> address_locks;
    //private LRUCache<Sha256Hash, Object> tx_locks;
    //private LRUCache<String, Object> tx_out_locks;
    private LRUCache<Sha256Hash, Transaction> transaction_cache;
    private LRUCache<Sha256Hash, Semaphore> in_progress;

    private HashSet<String> busy_addresses;

    private NetworkParameters params;

    private AtomicInteger imported_blocks= new AtomicInteger(0);
    private AtomicInteger imported_transactions= new AtomicInteger(0);

    //private BandedUpdater<String, Sha256Hash> address_to_tx_updater;

    private static final int BUSY_ADDRESS_LIMIT=10000;

    private int block_print_every=100;

    private volatile boolean run_rates=true;

    private LinkedList<StatusContext> save_thread_list;


    public Importer(NetworkParameters params, Jelectrum jelly, BlockStore block_store)
        throws com.google.bitcoin.store.BlockStoreException
    {
        this.jelly = jelly;
        this.params = params;
        this.file_db = jelly.getDB();
        this.block_store = (MapBlockStore)block_store;

        Config config = jelly.getConfig();
        config.require("block_save_threads");
        config.require("transaction_save_threads");


        block_queue = new LinkedBlockingQueue<Block>(32);
        tx_queue = new LinkedBlockingQueue<TransactionWork>(4096);
        transaction_cache = new LRUCache<Sha256Hash, Transaction>(100000);

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
        
        tryLoadBusyAddresses();


    }

    public void start()
    {
        new RateThread("1-minute", 60000L).start();
        new RateThread("5-minute", 60000L * 5L).start();
        new RateThread("1-hour", 60000L * 60L).start();
    }

    public void tryLoadBusyAddresses()
    {
        try
        {
            busy_addresses = (HashSet<String>) jelly.getDB().getSpecialObjectMap().get("BusyAddresses");
            System.out.println("Busy addresses: " + busy_addresses.size());

        }
        catch(Throwable t)
        {
            System.out.println("Creating new busy address set");
            busy_addresses = new HashSet<String>(100,0.5f);
        }
    }

    public void checkConsistency()
        throws com.google.bitcoin.store.BlockStoreException
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
        try
        {
            tx_queue.put(new TransactionWork(tx));
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
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

    public Transaction getTransaction(Sha256Hash hash)
    {
        Transaction tx = null;
        synchronized(transaction_cache)
        {
            tx = transaction_cache.get(hash);
        }
        if (tx == null)
        {

            SerializedTransaction s_tx = file_db.getTransactionMap().get(hash);

            if (s_tx != null)
            {
                tx = s_tx.getTx(params);
                synchronized(transaction_cache)
                {
                    transaction_cache.put(hash, SerializedTransaction.scrubTransaction(params, tx));
                }
            }

        }
        return tx;

    }

    public void putTxOutSpents(Transaction tx)
    {
        for(TransactionInput in : tx.getInputs())
        {
            if (!in.isCoinBase())
            {
                TransactionOutPoint out = in.getOutpoint();
                String key = out.getHash().toString() + ":" + out.getIndex();
                file_db.addTxOutSpentByMap(key, tx.getHash());

            }
        }
    }

    public void putInternal(Transaction tx, Sha256Hash block_hash)
    {
        putInternal(tx, block_hash, new NullStatusContext());
    }
    public void putInternal(Transaction tx, Sha256Hash block_hash, StatusContext ctx)
    {

        ctx.setStatus("TX_SERIALIZE");
        SerializedTransaction s_tx = new SerializedTransaction(tx);

        ctx.setStatus("TX_PUT");
        //System.out.println("Transaction " + tx.getHash() + " " + Util.measureSerialization(s_tx));
        file_db.getTransactionMap().put(tx.getHash(), s_tx);

        //putTxOutSpents(tx);
        boolean confirmed = (block_hash != null);

        ctx.setStatus("TX_GET_ADDR");
        Collection<String> addrs = getAllAddresses(tx, confirmed);

        Random rnd = new Random();

        for(String a : addrs)
        {
            ctx.setStatus("TX_ADDRESS_LOOP");
            boolean done=false;
            synchronized(busy_addresses)
            {
                if (busy_addresses.contains(a)) done=true;
            }

            if (!done)
            {
                long new_size = 0;
                ctx.setStatus("TX_SAVE_ADDRESS");
                file_db.addAddressToTxMap(a, tx.getHash());
                ctx.setStatus("TX_ADDRESS_LOOP");
                if (rnd.nextDouble() < 0.01)
                {
                    ctx.setStatus("TX_GET_ADDR_COUNT");
                    new_size = file_db.countAddressToTxSet(a);
                    ctx.setStatus("TX_ADDRESS_LOOP");
                }
                if (new_size >= BUSY_ADDRESS_LIMIT) 
                {
                    synchronized(busy_addresses)
                    {
                        busy_addresses.add(a);
                        jelly.getDB().getSpecialObjectMap().put("BusyAddresses", busy_addresses);
                    }
                }

            }

        }

        if (block_hash!=null)
        {
            ctx.setStatus("TX_BLOCK_SAVE");
            file_db.addTxToBlockMap(tx.getHash(), block_hash);
            ctx.setStatus("TX_ADDRESS_LOOP");
        }

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
        long t1 = System.currentTimeMillis();
        Sha256Hash hash = block.getHash();

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

        Semaphore sem = new Semaphore(0);
        //Kick off threaded storage of transactions
        int size=0;

        ctx.setStatus("BLOCK_TX_CACHE_INSERT");
        synchronized(transaction_cache)
        {
            for(Transaction tx : block.getTransactions())
            {
                transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params, tx));
            }
        }

        ctx.setStatus("BLOCK_TX_ENQUE");
        for(Transaction tx : block.getTransactions())
        {
            try
            {

                tx_queue.put(new TransactionWork(tx,sem,hash));
                size++;
            }
            catch(java.lang.InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        try
        {
            ctx.setStatus("BLOCK_TX_WAIT");
            int outstanding = size;
            long wait_start=System.currentTimeMillis();
            while(outstanding > 0)
            {
                if (sem.tryAcquire(60, TimeUnit.SECONDS))
                {
                    outstanding--;
                }
                else
                {
                    long t = System.currentTimeMillis();
                    double sec = (t - wait_start ) / 1000.0;
                    DecimalFormat df = new DecimalFormat("0.0");
                    System.out.println("Block " + hash + " still waiting ("+df.format(sec)+ " seconds) on " + outstanding + " transactions.");

                }


            }
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        

        //Once all transactions are in, check for prev block in this store

        ctx.setStatus("BLOCK_WAIT_PREV");
        Sha256Hash prev_hash = block.getPrevBlockHash();
        
        waitForBlockStored(prev_hash);

        //System.out.println("Block " + hash + " " + Util.measureSerialization(new SerializedBlock(block)));

        ctx.setStatus("BLOCK_SAVE");
        file_db.getBlockMap().put(hash, new SerializedBlock(block));

        block_wait_sem.release(1024);

        long t2 = System.currentTimeMillis();
        DecimalFormat df = new DecimalFormat("0.000");
        double sec = (t2 - t1)/1000.0;


        int h = block_store.getHeight(hash);
        if (h % block_print_every ==0)
        {
            System.out.println("Saved block: " + hash + " - " + h + " - " + size + " (" +df.format(sec) + " seconds)");
        }
        jelly.getEventLog().log("Saved block: " + hash + " - " + h + " - " + size + " (" +df.format(sec) + " seconds)");

        imported_blocks.incrementAndGet();

        jelly.getElectrumNotifier().notifyNewBlock(block);
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
 
    public Address getAddressForInput(TransactionInput in, boolean confirmed)
    {
        if (in.isCoinBase()) return null;

        try
        {
            Address a = in.getFromAddress();
            return a;
        }
        catch(ScriptException e)
        {
                    //Lets try this the other way

                    try
                    {

                        TransactionOutPoint out_p = in.getOutpoint();
                        
                        Transaction src_tx = null;
                        while(src_tx == null)
                        { 
                            src_tx = getTransaction(out_p.getHash());
                            if (src_tx == null)
                            {
                                if (!confirmed)
                                {
                                    return null;
                                }
                                System.out.println("Unable to get source transaction: " + out_p.getHash());
                                try{Thread.sleep(500);}catch(Exception e7){}
                            }
                        }
                        TransactionOutput out = src_tx.getOutput((int)out_p.getIndex());
                        Address a = getAddressForOutput(out);
                        return a;
                    }
                    catch(ScriptException e2)
                    {
                        return null;

                    }
        }

    }

    public Address getAddressForOutput(TransactionOutput out)
    {
            try
            {
                Script script = out.getScriptPubKey();
                if (script.isSentToRawPubKey())
                {
                    byte[] key = out.getScriptPubKey().getPubKey();
                    byte[] address_bytes = com.google.bitcoin.core.Utils.sha256hash160(key);
                    Address a = new Address(params, address_bytes);
                    return a;
                }
                else
                {
                    Address a = script.getToAddress(params);
                    return a;
                }
            }
            catch(ScriptException e)
            {

                //System.out.println(out.getParentTransaction().getHash() + " - " + out);
                //e.printStackTrace();
                jelly.getEventLog().log("Unable process tx output: " + out.getParentTransaction().getHash());
            }
            return null;
 
    }

    public Collection<String> getAllAddresses(Transaction tx, boolean confirmed)
    {
        HashSet<String> lst = new HashSet<String>();

        for(TransactionInput in : tx.getInputs())
        {
            Address a = getAddressForInput(in, confirmed);
            if (a!=null) lst.add(a.toString());
        }

        for(TransactionOutput out : tx.getOutputs())
        {
            Address a = getAddressForOutput(out);
            if (a!=null) lst.add(a.toString());

        }

        return lst;

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


                    if (name.equals("1-hour"))
                    {
                        //System.exit(0);
                    }
                    if (name.equals("5-minute"))
                    {
                        if (tx_rate < 50.0) System.exit(0);
                        jelly.getDB().open();
                    }
                    MongoMapSet.printStats();
                    String status_report = getThreadStatusReport();


                    System.out.println(status_report);
                    jelly.getEventLog().log(status_report);

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
        status_map.put("STALLED", 0);
        for(StatusContext t : save_thread_list)
        {
            String status = t.getStatus();
            if (!status_map.containsKey(status))
            {
                status_map.put(status, 0);
            }
            status_map.put(status, status_map.get(status) + 1);

            long age = System.currentTimeMillis() - t.getLastStatusChangeTime();
            if (age > 120000L)
            {
                status_map.put("STALLED", status_map.get("STALLED") + 1);
                
            }

        }
        return status_map.toString();

    }


}
