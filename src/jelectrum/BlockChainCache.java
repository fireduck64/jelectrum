package jelectrum;

import java.util.HashMap;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Map;
import java.util.ArrayList;

import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.store.BlockStore;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Maintains a mapping of integers to block hashes
 */
public class BlockChainCache
{
  private BlockStore store;
  private Map<Integer, Sha256Hash> height_map;
  private volatile Sha256Hash last_head;
  private EventLog event_log;


  public BlockChainCache(BlockStore store, Map<Integer, Sha256Hash> height_map, EventLog event_log)
  {
    this.store = store;
    this.height_map = height_map;
    this.event_log = event_log;
  }

  public void update(Jelectrum jelly, StoredBlock new_head)
      throws org.bitcoinj.store.BlockStoreException
  {
    last_head = new_head.getHeader().getHash();
    //event_log.log("BlockChainCache: chain update, new head: " + new_head.getHeader().getHash() + " - " + new_head.getHeight());

    Sha256Hash genesis_hash = jelly.getNetworkParameters().getGenesisBlock().getHash();

    StoredBlock cur = new_head;

    TreeMap<Integer, Sha256Hash> to_write = new TreeMap<>();

    int reorg=0;

    while(true)
    {
      int height = cur.getHeight();
      Sha256Hash curr_hash = cur.getHeader().getHash();

      Sha256Hash exist_hash = getBlockHashAtHeight(height);
      if ((exist_hash != null) && (!exist_hash.equals(curr_hash)))
      {
        reorg++;
      }

      if (curr_hash.equals(exist_hash)) break;

      to_write.put(height, curr_hash);
      if (curr_hash.equals(genesis_hash)) break;

      cur = cur.getPrev(store);

    }
    if (to_write.size() > 1)
    {
      event_log.log("BlockChainCache: adding " + to_write.size() + " to height map");
    }

    /**
     * Write them out in order to make sure this is recoverable if interupted in the middle
     */
    for(Map.Entry<Integer, Sha256Hash> me : to_write.entrySet())
    {
      height_map.put(me.getKey(), me.getValue());
    }
    if (reorg > 0)
    {
      event_log.alarm("BlockChainCache: re-org of " + reorg + " blocks found");
    }

  }

    public static BlockChainCache load(Jelectrum jelly)
    {
        return new BlockChainCache(jelly.getBlockStore(), jelly.getDB().getHeightMap(), jelly.getEventLog());
    }

    public Sha256Hash getBlockHashAtHeight(int height)
    {
      return height_map.get(height);
    }
    public boolean isBlockInMainChain(Sha256Hash hash)
    {
      try
      {
        StoredBlock sb = store.get(hash);
        int h = sb.getHeight();

        return (hash.equals(getBlockHashAtHeight(h)));
      }
      catch(org.bitcoinj.store.BlockStoreException e)
      {
        throw new RuntimeException(e);
      }

    }
    
    public Sha256Hash getHead()
    {
      return last_head;
    }

		public Sha256Hash getBlockTreeHash(int cp)
    {
			int v = 1;
      while(v < cp+1) v*=2;

			return getBlockTreeHash(0, cp+1, cp, v);

    }

    private HashMap<String, Sha256Hash> tree_hash_cache = new HashMap<>();

		/** inclusive on min, exclusive on max */
  	public Sha256Hash getBlockTreeHash(int min, int max, int cp, int sz)
  	{
      String key = null;
      if ((cp > max) && (sz > 8))
      {
        key = String.format("%d.%d.%d", min, max, sz);
        synchronized(tree_hash_cache)
        {
          if (tree_hash_cache.containsKey(key)) return tree_hash_cache.get(key);
        }
      }
			//System.out.println(String.format(" tree hash %d %d %d %d", min, max, cp, sz));
			if (sz == 1)
			{
				if (min > cp) return null; 
				return getBlockHashAtHeight(Math.min(min, cp));
			}

      Sha256Hash left =  getBlockTreeHash(min, min+sz/2, cp, sz/2);
      Sha256Hash right = getBlockTreeHash(min+sz/2, max, cp, sz/2);


      //System.out.println(String.format(" tree hash %d %d %d %d left: %s right: %s", min, max, cp, sz, left,right)); 

			if ((left == null) && (right == null)) return null;

      Sha256Hash ans = Util.treeHash(left, right);

      if (key != null)
      {
        synchronized(tree_hash_cache)
        {
          tree_hash_cache.put(key, ans);
        }
      }
      return ans;

  	}	

  public void populateBlockProof(Sha256Hash start_block, int start_height, int cp_height, JSONObject result)
  {
    JSONArray branch_list = new JSONArray();

    Sha256Hash cur_hash = start_block;
    result.put("root", getBlockTreeHash(cp_height));

		int sz=1;
    int tree_dance = start_height;
    int low = start_height;
    int high = start_height+1;
    
		while(sz < cp_height)
		{
      Sha256Hash other = null;
      if (tree_dance % 2 == 0)
      { // We are the left, need the right

        other = getBlockTreeHash(high,high+sz, cp_height, sz);
        high = high+sz;
        if (other == null) other = cur_hash;
        cur_hash = Util.treeHash(cur_hash, other);
      }
      else
      { // We are the right, need the left
        other=getBlockTreeHash(low-sz, low, cp_height, sz);
        low = low-sz;
        if (other == null) other= cur_hash;
        cur_hash = Util.treeHash(other, cur_hash);
      }
      branch_list.put(other);

      sz*=2;
      tree_dance /= 2;
		}


    result.put("branch", branch_list);
    /*int level = 1;

    while(cp_height > 0)
    {
      if (tree_dance % 2 == 0)
      { // We are the left, need the right

        cur_hash = Util.treeHash(cur_hash, getBlockTreeHash(cur_high+1, Math.mincp_height));

      }
      else
      { // We are the right, need the left
        cur_hash = Util.treeHash(getBlockTreeHash(0, cur_low-1));
      }

      cp_height /= 2;
      tree_dance /= 2;
      level++;
    }*/

  }


}
