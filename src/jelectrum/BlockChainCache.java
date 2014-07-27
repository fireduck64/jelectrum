package jelectrum;

import java.util.HashMap;
import java.util.HashSet;

import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;


public class BlockChainCache implements java.io.Serializable
{
    private HashMap<Integer, Sha256Hash> height_map;
    private HashSet<Sha256Hash> main_chain;
    private volatile Sha256Hash head;
    private transient Object update_lock=new Object();

    public static final int UPDATES_BEFORE_SAVE=100;
    private transient int updates = 0;
    

    public BlockChainCache()
    {
        height_map = new HashMap<Integer, Sha256Hash>(500000, 0.75f);
        main_chain = new HashSet<Sha256Hash>(500000, 0.75f);
        head=null;

    }

    private void retransient()
    {
        update_lock = new Object();
    }

    public void update(Jelectrum jelly, StoredBlock new_head)
        throws com.google.bitcoin.store.BlockStoreException
    {
        if (new_head.getHeader().getHash().equals(head)) return;

        Sha256Hash genisis_hash = jelly.getNetworkParameters().getGenesisBlock().getHash();

        synchronized(update_lock)
        {

            StoredBlock blk = new_head;

            while(true)
            {
                synchronized(this)
                {
                    int height = blk.getHeight();
                    Sha256Hash old = height_map.put(height, blk.getHeader().getHash());
                    if ((old!=null) && (old.equals(blk.getHeader().getHash())))
                    {
                        break;
                    }
                    if (old!=null)
                    {
                        main_chain.remove(old);
                    }
                    main_chain.add(blk.getHeader().getHash());
                    updates++;

                }
                if (blk.getHeader().getHash().equals(genisis_hash)) break;
                
                blk = blk.getPrev(jelly.getBlockStore());
    
            }

            head = new_head.getHeader().getHash();

            if (updates >= UPDATES_BEFORE_SAVE)
            {
                updates=0;
                save(jelly);

            }

        }


    }

    private void save(Jelectrum jelly)
    {
        jelly.getDB().getSpecialObjectMap().put("BlockChainCache", this);
        
    }
    public static BlockChainCache load(Jelectrum jelly)
    {
        try
        {

            BlockChainCache c = (BlockChainCache)jelly.getDB().getSpecialObjectMap().get("BlockChainCache");
            if (c!=null)
            {
                c.retransient();
                return c;
            }
            System.out.println("Creating new BlockChainCache");
            return new BlockChainCache();
        }
        catch(Throwable t)
        {
            t.printStackTrace();
            System.out.println("Error loading BlockChainCache.  Creating new.");
            return new BlockChainCache();
        }
    }

    public synchronized Sha256Hash getBlockHashAtHeight(int height)
    {
        return height_map.get(height);

    }
    public synchronized boolean isBlockInMainChain(Sha256Hash hash)
    {
        return main_chain.contains(hash);
    }

}
