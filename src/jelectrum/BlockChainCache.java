package jelectrum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;

import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;


public class BlockChainCache implements java.io.Serializable
{
    private HashMap<Integer, Sha256Hash> height_map;
    private HashSet<Sha256Hash> main_chain;
    private volatile Sha256Hash head;
    private transient Object update_lock=new Object();

    public static final int HEIGHT_BUCKETS=8;
    public static final int UPDATES_BEFORE_SAVE=100;
    private transient int updates = 0;
    

    public BlockChainCache()
    {
        height_map = new HashMap<Integer, Sha256Hash>(500000, 0.75f);
        main_chain = new HashSet<Sha256Hash>(500000, 0.75f);
        head=null;

    }
    public BlockChainCache(ArrayList<HashMap<Integer, Sha256Hash> > lst)
    {
        this();
        for(HashMap<Integer, Sha256Hash> m : lst)
        {
            height_map.putAll(m);
            main_chain.addAll(m.values());
        }
        int max_height = -1;
        for(Integer i : height_map.keySet())
        {
            max_height = Math.max(max_height, i);
        }
        if (max_height >= 0)
        head = height_map.get(max_height);


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
        //jelly.getDB().getSpecialObjectMap().put("BlockChainCache", this);

        ArrayList<HashMap<Integer, Sha256Hash> > height_map_list = new ArrayList<HashMap<Integer, Sha256Hash> >();
        for(int i=0; i<HEIGHT_BUCKETS; i++)
        {
            height_map_list.add(new HashMap<Integer, Sha256Hash>(height_map.size() * 2 / HEIGHT_BUCKETS + 1,0.5f));
        }
        for(Map.Entry<Integer, Sha256Hash> me : height_map.entrySet())
        {
            int h = me.getKey();
            Sha256Hash hash = me.getValue();
            int idx = h % HEIGHT_BUCKETS;
            height_map_list.get(idx).put(h, hash);
        }

        for(int i=0; i<HEIGHT_BUCKETS; i++)
        {
            jelly.getDB().getSpecialObjectMap().put("BlockChainCache_" + i, height_map_list.get(i));
        }

        
    }
    public static BlockChainCache load(Jelectrum jelly)
    {
        try
        {
            ArrayList<HashMap<Integer, Sha256Hash> > height_map_list = new ArrayList<HashMap<Integer, Sha256Hash> >();
            for(int i=0; i<HEIGHT_BUCKETS; i++)
            {
                HashMap<Integer, Sha256Hash> m = (HashMap<Integer, Sha256Hash>) jelly.getDB().getSpecialObjectMap().get("BlockChainCache_" + i);
                height_map_list.add(m);
            }

            BlockChainCache c = new BlockChainCache(height_map_list);
            return c;
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
