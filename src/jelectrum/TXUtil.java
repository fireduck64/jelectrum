package jelectrum;

import jelectrum.db.DBFace;

import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.core.TransactionOutPoint;
import com.google.bitcoin.core.Address;
import com.google.bitcoin.core.ScriptException;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.script.Script;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;

public class TXUtil
{
 
  private DBFace db;
  private NetworkParameters params;
  private LRUCache<Sha256Hash, Transaction> transaction_cache;

  public TXUtil(DBFace db, NetworkParameters params)
  {
    this.db = db;
    this.params = params;

  }

  public synchronized void saveTxCache(Transaction tx)
  {
    if (transaction_cache == null)
    {
      transaction_cache = new LRUCache<Sha256Hash, Transaction>(32000);
    }
    transaction_cache.put(tx.getHash(), tx);

  }
  public synchronized void putTxCacheIfOpen(Transaction tx)
  {
    if (transaction_cache != null)
    {
      transaction_cache.put(tx.getHash(), tx);
    }
  }

  public synchronized Transaction getTransaction(Sha256Hash hash)
  { 
    Transaction tx = null;
    if (transaction_cache != null) 
    {
      tx = transaction_cache.get(hash);
    }
    if (tx != null) return tx;

    SerializedTransaction s_tx = db.getTransaction(hash);

    if (s_tx != null)
    { 
      tx = s_tx.getTx(params);
      putTxCacheIfOpen(tx);
      return tx;
    }
    return null;
  }

    public Address getAddressForOutput(TransactionOutput out)
    {
            try
            {
                Script script = out.getScriptPubKey();
                if (script.isSentToRawPubKey())
                {
                    byte[] key = out.getScriptPubKey().getPubKey();
                    byte[] address_bytes = com.google.bitcoin.core.Utils.sha256hash160(key);
                    Address a = new Address(params, address_bytes);
                    return a;
                }
                else
                {
                    Address a = script.getToAddress(params);
                    return a;
                }
            }
            catch(ScriptException e)
            {
    
                //System.out.println(out.getParentTransaction().getHash() + " - " + out);
                //e.printStackTrace();
                //jelly.getEventLog().log("Unable process tx output: " + out.getParentTransaction().getHash());
            }
            return null;

    }

    public Collection<String> getAllAddresses(Transaction tx, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
    {   
        HashSet<String> lst = new HashSet<String>();
        boolean detail = false;

        for(TransactionInput in : tx.getInputs())
        {   
            Address a = getAddressForInput(in, confirmed, block_tx_map);
            if (a!=null) lst.add(a.toString());
        }

        for(TransactionOutput out : tx.getOutputs())
        {   
            Address a = getAddressForOutput(out);
            if (a!=null) lst.add(a.toString());

        }


        return lst;

    }

    public Address getAddressForInput(TransactionInput in, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
    {
        if (in.isCoinBase()) return null;

        try
        {
            Address a = in.getFromAddress();
            return a;
        }
        catch(ScriptException e)
        {
                    //Lets try this the other way

                    try
                    {

                        TransactionOutPoint out_p = in.getOutpoint();

                        Transaction src_tx = null;
                        while(src_tx == null)
                        {
                          if (block_tx_map != null)
                          { 
                            src_tx = block_tx_map.get(out_p.getHash());
                          }
                          if (src_tx == null)
                          { 
                            src_tx = getTransaction(out_p.getHash());
                            if (src_tx == null)
                            {   
                                if (!confirmed)
                                {   
                                    return null;
                                }
                                System.out.println("Unable to get source transaction: " + out_p.getHash());
                                try{Thread.sleep(500);}catch(Exception e7){}
                            }
                          }
                        }
                        TransactionOutput out = src_tx.getOutput((int)out_p.getIndex());
                        Address a = getAddressForOutput(out);
                        return a;
                    }
                    catch(ScriptException e2)
                    {   
                        return null;

                    }
        }

    }


}
