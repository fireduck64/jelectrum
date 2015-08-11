package jelectrum;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.DBCollection;
import com.mongodb.DB;

public class JelectrumDBMongo extends JelectrumDB
{
    private MongoClient mc;
    private MongoClient old_client;
    private DB db;
    private Config conf;
    protected MongoMapSet<String, Sha256Hash > address_to_tx_map;
    protected MongoMapSet<Sha256Hash, Sha256Hash > tx_to_block_map;

    public JelectrumDBMongo(Config conf)
        throws Exception
    {
        super(conf);

        conf.require("mongo_db_host");
        conf.require("mongo_db_compression");
        conf.require("mongo_db_name");
        conf.require("mongo_db_connections_per_host");
        this.conf = conf;

        open();

   }

    public synchronized void open()
    {

        try
        {
            if (old_client!=null) old_client.close();
            old_client = mc;
            

            boolean compress = conf.getBoolean("mongo_db_compression");


            MongoClientOptions.Builder opts = MongoClientOptions.builder();
            opts.connectionsPerHost(conf.getInt("mongo_db_connections_per_host"));

            // So with regards to https://github.com/fireduck64/jelectrum/issues/1
            // It seems that occasionally connections get into a bad state and just sit there
            // with the mongo client trying to read from them.
            // Testing seems to indicate that setting a socket timeout and a connection max lifetime
            // seems to fix this issue.  It doesn't explain it.
            //
            // However, as I have always said: life is too short to identify every mysterious
            // fluid that you encounter.
            //
            opts.maxConnectionLifeTime(120000);
            opts.socketTimeout(120000);

            mc = new MongoClient(conf.get("mongo_db_host"), opts.build());

            db = mc.getDB(conf.get("mongo_db_name"));

            tx_map = new MongoMap<Sha256Hash, SerializedTransaction>(getCollection("tx_map"), compress);
            address_to_tx_map = new MongoMapSet<String, Sha256Hash>(getCollection("address_to_tx_map"), compress);
            block_store_map = new CacheMap<Sha256Hash, StoredBlock>(25000,new MongoMap<Sha256Hash, StoredBlock>(getCollection("block_store_map"),compress));
            special_block_store_map = new MongoMap<String, StoredBlock>(getCollection("special_block_store_map"),compress);
            block_map = new CacheMap<Sha256Hash, SerializedBlock>(240,new MongoMap<Sha256Hash, SerializedBlock>(getCollection("block_map"),compress));
            tx_to_block_map = new MongoMapSet<Sha256Hash, Sha256Hash>(getCollection("tx_to_block_map"),compress);
            block_rescan_map = new MongoMap<Sha256Hash, String>(getCollection("block_rescan_map"),compress);
            special_object_map = new MongoMap<String, Object>(getCollection("special_object_map"),true);
            header_chunk_map = new CacheMap<Integer, String>(200, new MongoMap<Integer, String>(getCollection("header_chunk_map"),compress));

            utxo_trie_map = new CacheMap<String, UtxoTrieNode>(1000000, new MongoMap<String, UtxoTrieNode>(getCollection("utxo_trie_map"),compress));

        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }


    }

    private synchronized DBCollection getCollection(String name)
    {
        return db.getCollection(name);
    }

    public synchronized Map<Sha256Hash, StoredBlock> getBlockStoreMap()
    {   
        return block_store_map;
    }

    public synchronized Map<String, StoredBlock> getSpecialBlockStoreMap()
    {   
        return special_block_store_map;
    }

    public synchronized Map<Sha256Hash,SerializedTransaction> getTransactionMap()
    {   
        return tx_map;
    }
    public synchronized Map<Sha256Hash, SerializedBlock> getBlockMap()
    {   
        return block_map;
    }


    private synchronized MongoMapSet<String, Sha256Hash> getAddressToTxMap()
    {
        return address_to_tx_map;
    }

    public void addAddressToTxMap(String address, Sha256Hash hash)
    {
        getAddressToTxMap().add(address, hash);
    }
    public Set<Sha256Hash> getAddressToTxSet(String address)
    {
        return getAddressToTxMap().getSet(address);
    }

   
    private synchronized MongoMapSet<Sha256Hash, Sha256Hash> getTxToBlockMap()
    {
        return tx_to_block_map;
    }

  
    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
    {
        getTxToBlockMap().add(tx, block);
    }
    public Set<Sha256Hash> getTxToBlockMap(Sha256Hash tx)
    {
        return getTxToBlockMap().getSet(tx);
    }

 

    public synchronized Map<Sha256Hash, String> getBlockRescanMap()
    {
        return block_rescan_map;
    }

    public synchronized Map<String, Object> getSpecialObjectMap()
    {
        return special_object_map;
    }

    public synchronized Map<Integer, String> getHeaderChunkMap()
    {
        return header_chunk_map;
    }

}
