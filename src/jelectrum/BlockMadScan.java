package jelectrum;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.store.BlockStore;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Transaction;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.DecimalFormat;

import java.util.Collection;
import java.util.Set;

public class BlockMadScan
{
    public static void main(String args[]) throws Exception
    {
        new BlockMadScan(new Config(args[0]));

    }

    private Jelectrum jelly;
    private LinkedBlockingQueue<Sha256Hash> queue;
    private Semaphore sem;
    private AtomicInteger blocks_scanned;
    private AtomicInteger transactions_scanned;
    private String rescan_operation="zing-" + System.currentTimeMillis();
    private TXUtil tx_util;

    private LRUCache<String, Set<Sha256Hash> > addr_to_tx_cache;

    public BlockMadScan(Config config)
        throws Exception
    {
        jelly = new Jelectrum(config);

        tx_util = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());
        queue = new LinkedBlockingQueue<Sha256Hash>();
        sem = new Semaphore(0);
        blocks_scanned = new AtomicInteger(0);
        transactions_scanned = new AtomicInteger(0);
        addr_to_tx_cache = new LRUCache<String, Set<Sha256Hash> >(250000);


        BlockStore block_store = jelly.getBlockStore();

        //StoredBlock head = block_store.getChainHead();

        //StoredBlock curr_block = head;

        //Sha256Hash genisis_hash = jelly.getNetworkParameters().getGenesisBlock().getHash();
        int queued=0;

        new RateThread("1-minute", 60000L).start();
        new RateThread("5-minute", 60000L * 5L).start();
        new RateThread("1-hour", 60000L * 60L).start();

        for(int i=0; i<500; i++)
        {
            new WorkerThread().start();
        }

        int h =0;
        while(true)
        {
            Sha256Hash curr_hash = jelly.getBlockChainCache().getBlockHashAtHeight(h);
            if (curr_hash == null) break;
            queue.put(curr_hash);
            queued++;
            h++;
            
            if  (h % 10000 == 0)
            {
                System.out.println("Block: " + h);

            }
        }

        /*while(true)
        {
            Sha256Hash curr_hash = curr_block.getHeader().getHash();
            queue.put(curr_hash);

            if  (curr_block.getHeight() % 10000 == 0)
            {
                System.out.println("Block: " + curr_block.getHeight());
            }
            queued++;

            if (curr_hash.equals(genisis_hash)) break;

            curr_block = curr_block.getPrev(block_store);

        }*/
        System.out.println("Enqueued " + queued + " blocks");

        while(queued>0)
        {
            sem.acquire();
            queued--;
            if (queued % 1000 == 0) System.out.println("" + queued + " blocks left");
        }





    }

    public Set<Sha256Hash> getAddressToTxSet(String addr)
    {
      /*synchronized(addr_to_tx_cache)
      {
        if (addr_to_tx_cache.containsKey(addr)) return addr_to_tx_cache.get(addr);
      }*/
      Set<Sha256Hash> set = jelly.getDB().getAddressToTxSet(addr);
      /*synchronized(addr_to_tx_cache)
      {
        addr_to_tx_cache.put(addr, set);
      }*/
      return set;
    }

    public class WorkerThread extends Thread
    {
        public WorkerThread()
        {
            setName("WorkerThread");
            setDaemon(true);
        }
        public void run()
        {
            while(true)
            {
                Sha256Hash blk_hash = null;
                try
                {
                    blk_hash = queue.take();
                    String op = jelly.getDB().getBlockRescanMap().get(blk_hash);
                    if ((op == null) || (!op.equals(rescan_operation)))
                    {
                        Block blk = jelly.getDB().getBlockMap().get(blk_hash).getBlock(jelly.getNetworkParameters());
                        for(Transaction tx : blk.getTransactions())
                        {
                          SerializedTransaction tx2 = jelly.getDB().getTransactionMap().get(tx.getHash());
                          if (tx2 == null)
                          {
                            System.out.println("Transaction map does not contain " + tx.getHash());
                            System.exit(-1);
                          }
                          Collection<String> addresses = tx_util.getAllAddresses(tx, true, null);
                          for(String addr : addresses)
                          {
                            Set<Sha256Hash> tx_set = getAddressToTxSet(addr);
                            if (!tx_set.contains(tx.getHash()))
                            {
                              System.out.println("Address list for " + addr + " does not contain " + tx.getHash());
                              System.out.println(tx_set);
                              System.exit(-1);
                            }
                          }

                          Set<Sha256Hash> blk_set = jelly.getDB().getTxToBlockMap(tx.getHash());
                          if (!blk_set.contains(blk.getHash()))
                          {
                            System.out.println("TX Block list doesn't contain " + blk.getHash() + " for " + tx.getHash());
                            System.exit(-1);
                          }


                          transactions_scanned.incrementAndGet();
                        }
                        jelly.getDB().getBlockRescanMap().put(blk_hash, rescan_operation);
                    }
                    sem.release(1);
                    blocks_scanned.incrementAndGet();


                }
                catch(Throwable t)
                {
                    t.printStackTrace();
                    if (blk_hash != null) queue.offer(blk_hash);
                }
            }

        }


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
            setName("BlockMadScane/RateThread/"+name);

        }

        public void run()
        {
            long transactions = 0;
            long blocks = 0;
            long last_run = System.currentTimeMillis();
            DecimalFormat df =new DecimalFormat("0.000");
            while(true)
            {
                try{Thread.sleep(delay);}catch(Exception e){}

                long now = System.currentTimeMillis();
                long blocks_now = blocks_scanned.get();
                long transactions_now = transactions_scanned.get();

                double sec = (now - last_run) / 1000.0;

                double block_rate = (blocks_now - blocks) / sec;
                double tx_rate = (transactions_now - transactions) / sec;

                System.out.println(name + " Block rate: " + df.format(block_rate) + "/s   Transaction rate: " + df.format(tx_rate) + "/s");

                blocks = blocks_now;
                transactions = transactions_now;
                last_run= now;





            }

        }

    }


}
