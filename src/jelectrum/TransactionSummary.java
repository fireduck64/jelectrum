
package jelectrum;

import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;


import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

public class TransactionSummary implements java.io.Serializable
{
  private static final long serialVersionUID = 5333844648311484891L;
  private Sha256Hash tx_hash;
  private TreeMap<Integer, TransactionOutSummary> outs;
  private TreeMap<Integer, TransactionInSummary> ins;
  private HashSet<ByteString> involved_scripthashes;

  public TransactionSummary(Transaction tx, TXUtil tx_util, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
  {
    tx_hash = tx.getHash();
    involved_scripthashes = new HashSet<>();
    ins = new TreeMap<>();
    outs = new TreeMap<>();

    for(TransactionOutput tx_out : tx.getOutputs())
    {
      TransactionOutSummary os = new TransactionOutSummary(tx_out, tx_util);

      outs.put( tx_out.getIndex(), os );
      involved_scripthashes.add(os.getScriptHash());
    }
    int idx=0;
    for(TransactionInput tx_in : tx.getInputs())
    {
      TransactionInSummary is =  new TransactionInSummary(tx_in, tx_util, confirmed, block_tx_map);

      ByteString addr = is.getScriptHash();
      if (addr != null) involved_scripthashes.add(addr);

      ins.put(idx, is);  
      idx++;
    }
  }

  public Sha256Hash getHash() { return tx_hash; }
  public Map<Integer, TransactionOutSummary> getOutputs(){ return ImmutableMap.copyOf(outs); }
  public Map<Integer, TransactionInSummary> getInputs(){ return ImmutableMap.copyOf(ins); }
  public Set<ByteString> getScriptHashes(){ return ImmutableSet.copyOf(involved_scripthashes); }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("TX Summary: " + getHash()); sb.append('\n');
    sb.append("  IN: " + getInputs().toString()); sb.append('\n');
    sb.append("  OUT: " + getOutputs().toString()); sb.append('\n');
    sb.append("  ADDRESSES: " + getScriptHashes().toString()); sb.append('\n');

    return sb.toString();

  }

  public static class TransactionOutSummary implements java.io.Serializable
  {
    private long value;
    private ByteString scripthash;
        
    public TransactionOutSummary(TransactionOutput tx_out, TXUtil tx_util)
    {
      value = tx_out.getValue().getValue();

      scripthash = tx_util.getScriptHashForOutput(tx_out);

    }
    public ByteString getScriptHash()
    {
      return scripthash;
    }
    public long getValue()
    {
      return value;
    }
    public String toString()
    {
      return "" + scripthash + ":" + value;
    }

  }

  public class TransactionInSummary implements java.io.Serializable
  {
    private Sha256Hash tx_out_hash;
    private int tx_out_index;
    private boolean is_coinbase;
    private ByteString scripthash;

    public TransactionInSummary(TransactionInput tx_in, TXUtil tx_util, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
    {
      if (tx_in.isCoinBase())
      {
        is_coinbase=true;
        return;
      }

      tx_out_index = (int) tx_in.getOutpoint().getIndex();
      tx_out_hash = tx_in.getOutpoint().getHash();

      scripthash = tx_util.getScriptHashForInput(tx_in, confirmed, block_tx_map);
    }

    public Sha256Hash getTxOutHash(){return tx_out_hash;}
    public int getTxOutIndex(){return tx_out_index;}
    public boolean isCoinbase(){return is_coinbase;}
    public ByteString getScriptHash(){return scripthash;}

    public String toString()
    {
      if (is_coinbase) return "coinbase";

      return  ""+scripthash + " " + tx_out_hash + ":" + tx_out_index;
    }
  }

}
