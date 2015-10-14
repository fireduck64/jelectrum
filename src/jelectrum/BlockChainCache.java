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

}
