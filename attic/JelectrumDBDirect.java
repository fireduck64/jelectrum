package jelectrum;

import java.util.Map;
import java.util.HashSet;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;


public class JelectrumDBDirect extends JelectrumDB
{

    public JelectrumDBDirect(Config conf)
        throws java.io.IOException
    {
        super(conf);

        conf.require("db_direct_path");

        String base = conf.get("db_direct_path");
        tx_map = new DirectFileMap<Sha256Hash, SerializedTransaction>(base,"tx_map",4);

        address_to_tx_map = new DirectFileMap<String, HashSet<Sha256Hash> >(base,"address_to_tx_map", 4);
        block_store_map = new DirectFileMap<Sha256Hash, StoredBlock>(base,"block_store_map",4);
        special_block_store_map = new DirectFileMap<String, StoredBlock>(base,"special_block_store_map",4);
        block_map = new DirectFileMap<Sha256Hash, SerializedBlock>(base,"block_map",4);
        tx_to_block_map = new DirectFileMap<Sha256Hash, HashSet<Sha256Hash> >(base,"tx_to_block_map",4);

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
