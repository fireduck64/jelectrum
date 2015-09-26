
package jelectrum;

import java.util.HashSet;

import org.bitcoinj.store.BlockStore;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.NetworkParameters;
import java.util.List;
import java.util.TreeMap;
import java.util.HashMap;

import org.junit.Assert;

import jelectrum.db.DBFace;

public class MapBlockStore implements BlockStore
{
    private Jelectrum jelly;
    private DBFace file_db;

    private Sha256Hash genisis_hash;


    public MapBlockStore(Jelectrum jelly)
        throws org.bitcoinj.store.BlockStoreException
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

    public NetworkParameters getParams()
    {
      return jelly.getNetworkParameters();
    }

    public StoredBlock get(Sha256Hash hash)
    {
        StoredBlock b = file_db.getBlockStoreMap().get(hash);
        return b;
    }

    public StoredBlock getChainHead()
        throws org.bitcoinj.store.BlockStoreException
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
        throws org.bitcoinj.store.BlockStoreException
    {
        Sha256Hash hash = block.getHeader().getHash();

        file_db.getBlockStoreMap().put(hash, block);
    }

    public void putAll(List<Block> blks)
        throws org.bitcoinj.store.BlockStoreException
    {
      HashMap<Sha256Hash, StoredBlock> insert_map = new HashMap<>();

      StoredBlock last = null;

      for(Block b : blks)
      {
        Sha256Hash hash = b.getHash();
        Sha256Hash prev = b.getPrevBlockHash();

        if (!hash.equals(jelly.getNetworkParameters().getGenesisBlock().getHash()))
        {
          StoredBlock prev_sb = insert_map.get(prev);
          if (prev_sb == null)
          {
            prev_sb = file_db.getBlockStoreMap().get(prev);
          }
          Assert.assertNotNull(prev_sb);

          Block header = b.cloneAsHeader();
          StoredBlock sb = prev_sb.build(header);

          last = sb;

          insert_map.put(hash, sb);
        }
      }


      file_db.getBlockStoreMap().putAll(insert_map);

      if (last != null) saveChainHead(last);

    }

    public int getHeight(Sha256Hash hash)
    {
        return get(hash).getHeight();
    }


    public void setChainHead(StoredBlock block)
        throws org.bitcoinj.store.BlockStoreException
    {
        Sha256Hash hash = block.getHeader().getHash();


        //if (jelly.isUpToDate() || (block.getHeight() % 100 == 0))
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


    private void saveChainHead(StoredBlock block)
        throws org.bitcoinj.store.BlockStoreException
    {
      Sha256Hash hash = block.getHeader().getHash();
      file_db.getSpecialBlockStoreMap().put("head", block);

        if (jelly.getBlockChainCache() != null)
        {
            jelly.getBlockChainCache().update(jelly, block);
        }
    }


   


}
