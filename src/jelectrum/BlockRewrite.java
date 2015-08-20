package jelectrum;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Transaction;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.DecimalFormat;

public class BlockRewrite
{
    public static void main(String args[]) throws Exception
    {
        new BlockRewrite(new Config(args[0]));

    }

    private Jelectrum jelly;
    private LinkedBlockingQueue<Sha256Hash> queue;
    private Semaphore sem;
    private AtomicInteger blocks_scanned;
    private AtomicInteger transactions_scanned;
    private String rescan_operation="x" + System.currentTimeMillis();

    public BlockRewrite(Config config)
        throws Exception
    {
        jelly = new Jelectrum(config);
        queue = new LinkedBlockingQueue<Sha256Hash>();
        sem = new Semaphore(0);
        blocks_scanned = new AtomicInteger(0);
        transactions_scanned = new AtomicInteger(0);

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
                            //jelly.getDB().getTransactionMap().put(tx.getHash(), new SerializedTransaction(tx));
                            //jelly.getImporter().putTxOutSpents(tx);
                            jelly.getImporter().putInternal(tx, blk.getHash());
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
            setName("BlockRewritee/RateThread/"+name);

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
