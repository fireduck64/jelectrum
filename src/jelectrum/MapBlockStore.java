
package jelectrum;

import java.util.HashSet;

import com.google.bitcoin.store.BlockStore;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.NetworkParameters;

import jelectrum.db.DBFace;

public class MapBlockStore implements BlockStore
{
    private Jelectrum jelly;
    private DBFace file_db;

    private Sha256Hash genisis_hash;


    public MapBlockStore(Jelectrum jelly)
        throws com.google.bitcoin.store.BlockStoreException
    {
        this.jelly = jelly;

        file_db = jelly.getDB();
        NetworkParameters params = jelly.getNetworkParameters();
        
        this.file_db = file_db;


        genisis_hash = params.getGenesisBlock().getHash();


        if (!file_db.getSpecialBlockStoreMap().containsKey("head"))
        {
        
            System.out.println("Adding genesis");
            Block genesisHeader = params.getGenesisBlock().cloneAsHeader();
            StoredBlock storedGenesis = new StoredBlock(genesisHeader, genesisHeader.getWork(), 0);
            put(storedGenesis);
            setChainHead(storedGenesis);
        }

    }


    public void close()
    {
        file_db.commit();
    }

    public StoredBlock get(Sha256Hash hash)
    {
        StoredBlock b = file_db.getBlockStoreMap().get(hash);
        return b;
    }

    public StoredBlock getChainHead()
        throws com.google.bitcoin.store.BlockStoreException
    {
        System.out.print("GET HEAD - ");
        StoredBlock head_blk =  file_db.getSpecialBlockStoreMap().get("head");


        StoredBlock curr = head_blk;
        int stepback=0;
        if (file_db.getBlockMap()==null) throw new RuntimeException("BlockMap is null");

        while((!file_db.getBlockMap().containsKey(curr.getHeader().getHash())) && (curr.getHeight()>=1))
        {   
            int step_size=250;
            if (curr.getHeight() < 1000) step_size=1;
            for(int i=0; i<step_size; i++)
            {
                stepback++;
                curr = curr.getPrev(this);
            }
        }
        System.out.println(curr.getHeader().getHash().toString() + " - " + curr.getHeight() + " stepback " + stepback);
        return curr;
    }

    public void put(StoredBlock block)
        throws com.google.bitcoin.store.BlockStoreException
    {
        Sha256Hash hash = block.getHeader().getHash();

        file_db.getBlockStoreMap().put(hash, block);
    }

    public int getHeight(Sha256Hash hash)
    {
        return get(hash).getHeight();
    }


    public void setChainHead(StoredBlock block)
        throws com.google.bitcoin.store.BlockStoreException
    {
        Sha256Hash hash = block.getHeader().getHash();


        if (jelly.isUpToDate() || (block.getHeight() % 100 == 0))
        {
          file_db.getSpecialBlockStoreMap().put("head", block);
        }

        if (jelly.getBlockChainCache() != null)
        {
            jelly.getBlockChainCache().update(jelly, block);
        }

        if (jelly.getHeaderChunkAgent()!=null)
        {
            jelly.getHeaderChunkAgent().poke(block.getHeight());
        }

    }


   


}
