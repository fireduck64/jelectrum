package jelectrum;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.HashMultimap;
import com.google.protobuf.ByteString;

/**
 * The dolphin of this class is to fetch and maintain a view of the current mempool.
 *
 * This is done by polling (or being triggered to poll) and using bitcoin rpc to get
 * a list of the current mempool transactions.  Then any unknown transactions will be loaded.
 *
 * mempool transactions are kept in memory and not saved to the DB
 */
public class MemPooler extends Thread
{
  private Jelectrum jelly;

  private MemPoolInfo latest_info;


  public MemPooler(Jelectrum jelly)
  {
    this.jelly = jelly;
    setName("MemPooler");
    setDaemon(true);

  }

  public void run()
  {
    while(true)
    {
      try
      {
         runInner();
      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("Error in MemPooler: " + t.toString());
        jelly.getEventLog().logTrace(t);
      }
      try
      {
        Thread.sleep(15000L);
      }
      catch(Throwable t)
      {
        throw new RuntimeException(t);
      }

    }
  }

  public void triggerUpdate()
    throws Exception
  {
    //TODO - implement
  }
  private void runInner()
    throws Exception
  {
    HashSet<Sha256Hash> new_tx_set = new HashSet<>();

    new_tx_set.addAll(jelly.getBitcoinRPC().getMempoolList());

    MemPoolInfo prev_info = latest_info;

    MemPoolInfo new_info = new MemPoolInfo();

    new_info.tx_set.addAll(new_tx_set);
    int existing_tx=0;
    int new_tx=0;
    int fail_tx=0;

    HashSet<ByteString> new_keys = new HashSet<>();

    for(Sha256Hash tx_hash : new_tx_set)
    {
      TransactionSummary tx_summary = null;
      if (prev_info !=null)
      {
        tx_summary = prev_info.tx_summary_map.get(tx_hash);
        existing_tx++;
      }
      if (tx_summary == null)
      {
        Transaction tx = jelly.getDB().getTXUtil().getTransaction(tx_hash);
        if (tx != null)
        {
          tx_summary = new TransactionSummary(tx, jelly.getDB().getTXUtil(), false, null);
          new_tx++;
  
          new_keys.addAll(tx_summary.getPublicKeys());
        }
        else
        {
          jelly.getEventLog().log(String.format("MemPooler: Failed to load TX - %s", tx_hash.toString()));
        }
      }

      if (tx_summary == null)
      {
        fail_tx++;
      }
      else
      {
        new_info.tx_summary_map.put(tx_hash, tx_summary);

        for(ByteString pubkey : tx_summary.getPublicKeys())
        {
          new_info.pubkey_to_tx_map.put(pubkey, tx_hash);
        }
      }

    }

    jelly.getEventLog().log(String.format("Mempool size: %d (existing %d, new %d, fail %d)", new_tx_set.size(), existing_tx, new_tx, fail_tx));

    latest_info = new_info;

    jelly.getElectrumNotifier().notifyNewTransactionKeys(new_keys, -1);
  }

  public HashSet<Sha256Hash> getTxForPubKey(ByteString key)
  {
    HashSet<Sha256Hash> set = new HashSet<>();
    MemPoolInfo info = latest_info;
    if (info == null) return set;

    Collection<Sha256Hash> mem_set = info.pubkey_to_tx_map.get(key);
    if (mem_set != null)
    {
      set.addAll(mem_set);
    } 
    return set;
  }
  public boolean areSomeInputsPending(Transaction tx)
  {
    MemPoolInfo info = latest_info;
    if (info == null) return false; //Hard to say

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        TransactionOutPoint tx_out = tx_in.getOutpoint();
        Sha256Hash parent_hash = tx_out.getHash();
        if (info.tx_set.contains(parent_hash)) return true;
      }
    }
    return false;
  }

  public class MemPoolInfo
  {
    HashSet<Sha256Hash> tx_set;
    HashMap<Sha256Hash, TransactionSummary> tx_summary_map;
    Multimap<ByteString, Sha256Hash> pubkey_to_tx_map;

    public MemPoolInfo()
    {
      tx_set = new HashSet<>(256, 0.5f);
      tx_summary_map = new HashMap<>(256, 0.5f);
      pubkey_to_tx_map = HashMultimap.<ByteString,Sha256Hash>create();
    }

  }

}
