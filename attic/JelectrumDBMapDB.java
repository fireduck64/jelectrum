package jelectrum;

import org.mapdb.DBMaker;
import org.mapdb.DB;
import java.io.File;
import java.util.Map;
import java.util.HashSet;
import java.text.DecimalFormat;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JelectrumDBMapDB extends JelectrumDB
{
    private volatile DB db;

    ReentrantReadWriteLock hunch_lock;


    public JelectrumDBMapDB(Config conf)
    {
        super(conf);

        hunch_lock = new ReentrantReadWriteLock(true);

        openDb();
    }

    private void openDb()
    {
        String path = conf.get("db_path");

        db = DBMaker.newFileDB(new File(path + "/jelectrum"))
            .closeOnJvmShutdown()
            //.checksumEnable()
            //.compressionEnable()
            //.transactionDisable()
            //.mmapFileEnable()
            .make();

        //tx_map = db.getHashMap("tx_map");
        tx_map = new FragMap<Sha256Hash, SerializedTransaction>(db, "tx_map", 64);
        //address_to_tx_map = db.getHashMap("address_to_tx_map");
        address_to_tx_map = new FragMap<String, HashSet<Sha256Hash> >(db, "address_to_tx_map", 64);
        block_store_map = db.getHashMap("block_store_map");
        special_block_store_map = db.getHashMap("special_block_store_map");
        block_map = db.getHashMap("block_map");
        //tx_to_block_map = db.getHashMap("tx_to_block_map");
        tx_to_block_map = new FragMap<Sha256Hash, HashSet<Sha256Hash> >(db, "tx_to_block_map", 64);

        //db.compact();

        //System.out.println("Blocks: " + block_map.size());
        //System.out.println("Transactions: " + tx_map.size());
        //System.out.println("Addresses: " + address_to_tx_map.size());

    }
    public void compact()
    {
        long t1 = System.currentTimeMillis();
        System.out.println("Compact called");
        db.compact();
        long t2 = System.currentTimeMillis();
        double sec = (t2 - t1) / 1000.0;
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("Compact completed in " + df.format(sec) + " seconds");
    }
 

    public void commit()
    {
        long t1 = System.currentTimeMillis();
        System.out.println("Commit called");

        hunch_lock.writeLock().lock();
        System.out.println("Commit has lock");
        db.commit();
        System.out.println("Reopening DB");
        db.close();
        openDb();
        try
        {
        Thread.sleep(5000);
        }
        catch(Exception e){throw new RuntimeException(e);}
        hunch_lock.writeLock().unlock();
        long t2 = System.currentTimeMillis();
        double sec = (t2 - t1) / 1000.0;
        DecimalFormat df = new DecimalFormat("0.000");
        System.out.println("Commit completed in " + df.format(sec) + " seconds");
    }
    public void close()
    {
        db.close();
    }

    public Map<Sha256Hash, StoredBlock> getBlockStoreMap()
    {
        return block_store_map;
    }

    public Map<String, StoredBlock> getSpecialBlockStoreMap()
    {
        return special_block_store_map;
    }

    public Map<Sha256Hash,SerializedTransaction> getTransactionMap()
    {
        return tx_map;
    }
    public Map<Sha256Hash, SerializedBlock> getBlockMap()
    {
        return block_map;
    }

    public Map<String, HashSet<Sha256Hash> > getAddressToTxMap()
    {
        return address_to_tx_map;
    }

    public Map<Sha256Hash, HashSet<Sha256Hash> > getTxToBlockMap()
    {
        return tx_to_block_map;
    }


}
