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

    private LRUCache<String, Boolean> busy_addresses;

    private NetworkParameters params;

    private AtomicInteger imported_blocks= new AtomicInteger(0);
    private AtomicInteger imported_transactions= new AtomicInteger(0);

    //private BandedUpdater<String, Sha256Hash> address_to_tx_updater;

    private static final int BUSY_ADDRESS_LIMIT=10000;

    private int block_print_every=100;

    private volatile boolean run_rates=true;


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
        //address_locks = new LRUCache<String, Object>(10000);
        //tx_locks = new LRUCache<Sha256Hash, Object>(10000);
        //tx_out_locks = new LRUCache<String, Object>(10000);
        transaction_cache = new LRUCache<Sha256Hash, Transaction>(100000);

        in_progress = new LRUCache<Sha256Hash, Semaphore>(1024);

        busy_addresses = new LRUCache<String, Boolean>(10000);

        //address_to_tx_updater = new BandedUpdater<String, Sha256Hash>(file_db.getAddressToTxMap(), config.getInt("transaction_save_threads")/2);

        for(int i=0; i<config.getInt("block_save_threads"); i++)
        {
            new BlockSaveThread().start();
        }
        for(int i=0; i<config.getInt("transaction_save_threads"); i++)
        {
            new TransactionSaveThread().start();
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
            if (checked > 20) return;
            
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

  public class BlockSaveThread extends Thread
    {
        public BlockSaveThread()
        {
            setDaemon(true);
            setName("BlockSaveThread");
        }

        public void run()
        {
            while(true)
            {
                try
                {
                    Block blk = block_queue.take();

                    putInternal(blk);
                }
                catch(Throwable e)
                {
                    e.printStackTrace();
                }

            }
        }
    }


    public class TransactionSaveThread extends Thread
    {
        public TransactionSaveThread()
        {
            setDaemon(true);
            setName("TransactionSaveThread");
        }
        public void run()
        {
            while(true)
            {
                try
                {
                    TransactionWork tw = tx_queue.take();

                    while(true)
                    {
                        try
                        {

                            putInternal(tw.tx, tw.block_hash);
                            break;
                        }
                        catch(java.lang.IllegalAccessError e3)
                        {
                            e3.printStackTrace();
                            System.exit(-1);
                        }
                        catch(Throwable e2)
                        {
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
        SerializedTransaction s_tx = new SerializedTransaction(tx);
        //System.out.println("Transaction " + tx.getHash() + " " + Util.measureSerialization(s_tx));
        file_db.getTransactionMap().put(tx.getHash(), s_tx);

        //putTxOutSpents(tx);
        boolean confirmed = (block_hash != null);

        Collection<String> addrs = getAllAddresses(tx, confirmed);

        for(String a : addrs)
        {
            boolean done=false;
            synchronized(busy_addresses)
            {
                if (busy_addresses.containsKey(a)) done=true;
            }

            if (!done)
            {
                int new_size = 0;
                file_db.addAddressToTxMap(a, tx.getHash());
                if (new_size >= BUSY_ADDRESS_LIMIT) 
                {
                    boolean print=false;
                    synchronized(busy_addresses)
                    {
                        if (!busy_addresses.containsKey(a)) print=true;

                        busy_addresses.put(a,true);
                    }
                }

            }

        }

        if (block_hash!=null)
        {
            file_db.addTxToBlockMap(tx.getHash(), block_hash);
        }

        imported_transactions.incrementAndGet();
        int h = -1;
        if (block_hash != null)
        {
            h = block_store.getHeight(block_hash);
        }



        jelly.getElectrumNotifier().notifyNewTransaction(tx, addrs, h);
        

    }
   
    private void putInternal(Block block)
    {
        long t1 = System.currentTimeMillis();
        Sha256Hash hash = block.getHash();

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

        synchronized(transaction_cache)
        {
            for(Transaction tx : block.getTransactions())
            {
                transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params, tx));
            }
        }

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

        Sha256Hash prev_hash = block.getPrevBlockHash();
        
        waitForBlockStored(prev_hash);

        //System.out.println("Block " + hash + " " + Util.measureSerialization(new SerializedBlock(block)));

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

                    System.out.println(name + " Block rate: " + df.format(block_rate) + "/s   Transaction rate: " + df.format(tx_rate) + "/s" + "     txq:" + tx_queue.size() + " blkq:" + block_queue.size()  );

                    if (name.equals("1-hour"))
                    {
                        //System.exit(0);
                    }
                    if (name.equals("5-minute"))
                    {
                        //jelly.getDB().open();
                    }
                    MongoMap.printStats();
                }


                blocks = blocks_now;
                transactions = transactions_now;
                last_run= now;


                



            }

        }

    }

}
