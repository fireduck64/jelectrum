
package jelectrum;

import java.util.TreeMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;


public class TransactionSummary implements java.io.Serializable
{

  private Sha256Hash tx_hash;
  private TreeMap<Integer, TransactionOutSummary> outs;
  private TreeMap<Integer, TransactionInSummary> ins;
  private TreeSet<String> involved_addresses;

  public TransactionSummary(Transaction tx, TXUtil tx_util, Map<Sha256Hash, TransactionSummary> block_tx_map)
  {
    tx_hash = tx.getHash();
    involved_addresses = new TreeSet<>();
    ins = new TreeMap<>();
    outs = new TreeMap<>();

    for(TransactionOutput tx_out : tx.getOutputs())
    {
      TransactionOutSummary os = new TransactionOutSummary(tx_out, tx_util);

      outs.put( tx_out.getIndex(), os );
      String addr = os.getToAddress();
      if (addr != null) involved_addresses.add(addr);
    }
    int idx=0;
    for(TransactionInput tx_in : tx.getInputs())
    {
      TransactionInSummary is =  new TransactionInSummary(tx_in, tx_util, block_tx_map);

      String addr = is.getFromAddress();
      if (addr != null) involved_addresses.add(addr);

      ins.put(idx, is);  
      idx++;
    }
  }
  public Sha256Hash getHash() { return tx_hash; }
  public Map<Integer, TransactionOutSummary> getOutputs(){ return ImmutableMap.copyOf(outs); }
  public Map<Integer, TransactionInSummary> getInputs(){ return ImmutableMap.copyOf(ins); }
  public Set<String> getAddresses(){ return ImmutableSet.copyOf(involved_addresses); }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("TX Summary: " + getHash()); sb.append('\n');
    sb.append("  IN: " + getInputs().toString()); sb.append('\n');
    sb.append("  OUT: " + getOutputs().toString()); sb.append('\n');
    sb.append("  ADDRESSES: " + getAddresses().toString()); sb.append('\n');

    return sb.toString();

  }

  public static class TransactionOutSummary implements java.io.Serializable
  {
    private long value;
    private String to_address;
    
    
    public TransactionOutSummary(TransactionOutput tx_out, TXUtil tx_util)
    {
      value = tx_out.getValue().getValue();

      Address addr = tx_util.getAddressForOutput(tx_out);
      if (addr != null)
      {
        to_address = addr.toString();
      }

    }
    public String getToAddress()
    {
      return to_address;
    }
    public long getValue()
    {
      return value;
    }
    public String toString()
    {
      return "" + to_address + ":" + value;
    }

  }

  public class TransactionInSummary implements java.io.Serializable
  {
    private Sha256Hash tx_out_hash;
    private int tx_out_index;
    private boolean is_coinbase;
    private String from_address;

    public TransactionInSummary(TransactionInput tx_in, TXUtil tx_util, Map<Sha256Hash, TransactionSummary> block_tx_map)
    {
      if (tx_in.isCoinBase())
      {
        is_coinbase=true;
        return;
      }

      tx_out_index = (int) tx_in.getOutpoint().getIndex();
      tx_out_hash = tx_in.getOutpoint().getHash();

      from_address = tx_util.getAddressForInputViaSummary(tx_in, true, block_tx_map);
    }

    public Sha256Hash getTxOutHash(){return tx_out_hash;}
    public int getTxOutIndex(){return tx_out_index;}
    public boolean isCoinbase(){return is_coinbase;}
    public String getFromAddress(){return from_address;}

    public String toString()
    {
      if (is_coinbase) return "coinbase";

      return  ""+from_address + " " + tx_out_hash + ":" + tx_out_index;
    }
  }

}
