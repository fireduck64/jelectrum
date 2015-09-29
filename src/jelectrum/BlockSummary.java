package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Block;
import com.google.common.collect.ImmutableMap;

import jelectrum.proto.Summary;

import org.junit.Assert;

public class BlockSummary implements java.io.Serializable
{
  private static final long serialVersionUID = 8859834861291838970L;

  private int height;
  private Sha256Hash block_hash;
  private HashMap<Sha256Hash, TransactionSummary> tx_map;

  public BlockSummary(int height, Block blk, TXUtil tx_util, Map<Sha256Hash, TransactionSummary> tx_cache)
  {
    this.height = height;
    this.block_hash = blk.getHash();

    tx_map = new HashMap<>();

    for(Transaction tx : blk.getTransactions())
    {
      TransactionSummary tx_sum = new TransactionSummary(tx, tx_util, tx_cache);

      tx_map.put(tx.getHash(), tx_sum);
      synchronized(tx_cache)
      {
        tx_cache.put(tx.getHash(), tx_sum);
      }

    }
  }
  public int getHeight() { return height; }
  public Sha256Hash getHash() { return block_hash; }
  public Map<Sha256Hash, TransactionSummary> getTxMap() { return ImmutableMap.copyOf(tx_map);} 



  public Summary.BitcoinBlockSummary getProto()
  {
    Summary.BitcoinBlockSummary.Builder block_builder = Summary.BitcoinBlockSummary.newBuilder();
    if (getHash() != null)
    {
      block_builder.setBlockHash(getHash().toString());
    }
    block_builder.setHeight(getHeight());

    Map<String,Summary.TransactionSummary> build_map = block_builder.getMutableTx();
    for(Map.Entry<Sha256Hash, TransactionSummary> me : tx_map.entrySet())
    {
      build_map.put(me.getKey().toString(), me.getValue().getProto());
      
    }
    return block_builder.build();
  }

  public Set<String> getAllAddresses()
  {
    HashSet<String> set = new HashSet<>();

    for(TransactionSummary tx : tx_map.values())
    {
      set.add(tx.getHash().toString());
      set.addAll(tx.getAddresses());
    }
    return set;
  }

  public Set<Sha256Hash> getMatchingTransactions(String address)
  {
    Set<Sha256Hash> set = new HashSet<>();

    for(TransactionSummary tx : tx_map.values())
    {
      if (tx.getAddresses().contains(address))
      {
        set.add(tx.getHash());
      }
    }
    return set;
  }
}
