package jelectrum;

import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.Collection;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;


public class JelectrumDBSQL extends JelectrumDB
{
    private Config conf;
    protected SqlMapSet<String> address_to_tx_map;
    protected SqlMapSet<Sha256Hash> tx_to_block_map;
    protected boolean compress=false;

    public JelectrumDBSQL(Config config)
        throws Exception
    {
        super(config);

        config.require("sql_db_driver");
        config.require("sql_db_uri");
        config.require("sql_db_username");
        config.require("sql_db_password");
        config.require("sql_db_conns");

        DB.openConnectionPool(
            "jelectrum_db",
            config.get("sql_db_driver"),
            config.get("sql_db_uri"),
            config.get("sql_db_username"),
            config.get("sql_db_password"),
            config.getInt("sql_db_conns"),
            16);

        this.conf = config;

        open();

   }

    public synchronized void open()
    {

        try
        {

            //tx_map = new BandingMap<Sha256Hash, SerializedTransaction>(new SqlMap<Sha256Hash, SerializedTransaction>("tx_map", 64),100);
            tx_map = new SqlMap<Sha256Hash, SerializedTransaction>("tx_map", 64);
            block_store_map = new CacheMap<Sha256Hash, StoredBlock>(25000,new SqlMap<Sha256Hash, StoredBlock>("block_store_map",64));
            special_block_store_map = new SqlMap<String, StoredBlock>("special_block_store_map",128);
            block_map = new CacheMap<Sha256Hash, SerializedBlock>(240,new SqlMap<Sha256Hash, SerializedBlock>("block_map",64));
            block_rescan_map = new SqlMap<Sha256Hash, String>("block_rescan_map",64);
            special_object_map = new SqlMap<String, Object>("special_object_map",128);
            header_chunk_map = new CacheMap<Integer, String>(200, new SqlMap<Integer, String>("header_chunk_map",32));
            utxo_trie_map = new CacheMap<String, UtxoTrieNode>(10000, new SqlMap<String, UtxoTrieNode>("utxo_trie_map",56*2));

            //address_to_tx_map = new BandingMapSet<String, Sha256Hash>(new SqlMapSet<String>("address_to_tx_map",35),10);
            //tx_to_block_map = new BandingMapSet<Sha256Hash, Sha256Hash>(new SqlMapSet<Sha256Hash>("tx_to_block_map",64),10);
            //txout_spent_by_map = new BandingMapSet<String, Sha256Hash>(new SqlMapSet<String>("txout_spent_by_map",128),10);
            address_to_tx_map = new SqlMapSet<String>("address_to_tx_map",35);
            tx_to_block_map = new SqlMapSet<Sha256Hash>("tx_to_block_map",64);

        }
        catch(Exception e)
        {
            throw new RuntimeException(e);
        }


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


    private synchronized MapSet<String,Sha256Hash> getAddressToTxMap()
    {
        return address_to_tx_map;
    }

    public void addAddressToTxMap(String address, Sha256Hash hash)
    {
        getAddressToTxMap().add(address, hash);
    }
    @Override
    public void addAddressesToTxMap(Collection<String> addresses, Sha256Hash hash)
    {
      address_to_tx_map.addAll(addresses, hash);
    }
    @Override
    public void addAddressesToTxMap(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      address_to_tx_map.addAll(lst);
    }
    public Set<Sha256Hash> getAddressToTxSet(String address)
    {
        return getAddressToTxMap().getSet(address);
    }

   
    private synchronized MapSet<Sha256Hash,Sha256Hash> getTxToBlockMap()
    {
        return tx_to_block_map;
    }

  
    public void addTxToBlockMap(Sha256Hash tx, Sha256Hash block)
    {
        getTxToBlockMap().add(tx, block);
    }
    @Override
    public void addTxsToBlockMap(Collection<Sha256Hash> txs, Sha256Hash block)
    {
      tx_to_block_map.addAll(txs, block);
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
