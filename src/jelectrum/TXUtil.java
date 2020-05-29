package jelectrum;

import jelectrum.db.DBFace;

import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.Address;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.script.Script;
import org.bitcoinj.core.Coin;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.Set;
import com.google.protobuf.ByteString;

import org.junit.Assert;

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
      transaction_cache = new LRUCache<Sha256Hash, Transaction>(64000);
    }
    transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params,tx));

  }
  public synchronized void putTxCacheIfOpen(Transaction tx)
  {
    if (transaction_cache != null)
    {
     transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params,tx));
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

  public ByteString getScriptHashForOutput(TransactionOutput out)
  {
    //System.out.println("Out Script: " + Util.getHexString(ByteString.copyFrom(out.getScriptBytes())));
    //System.out.println("Out Script: " + out.getScriptPubKey());
    return Util.reverse(Util.SHA256BIN(ByteString.copyFrom(out.getScriptBytes())));
  }
  public ByteString getScriptHashForInput(TransactionInput in, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
  {
    //System.out.println("In Script: " + Util.getHexString(ByteString.copyFrom(in.getScriptBytes())));
    //System.out.println("In Script: " + in.  getScriptSig());
    if (in.isCoinBase()) return null;

    /*try
    {
      Address a = in.getFromAddress();
      return getScriptHashForAddress(a.toString());
    }
    catch(ScriptException e)
    {*/
      //Lets try this the other way
      try
      {

        TransactionOutPoint out_p = in.getOutpoint();

        Transaction src_tx = null;
        int fail_count =0;
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
              fail_count++;
              if (fail_count > 30)
              {
                System.out.println("Unable to get source transaction: " + out_p.getHash());
              }
              if (fail_count > 240)
              {
                throw new RuntimeException("Waited too long to get transaction: " + out_p.getHash());
              }
              try{Thread.sleep(500);}catch(Exception e7){}
            }
          }
        }
        
        TransactionOutput out = src_tx.getOutput((int)out_p.getIndex());
        return getScriptHashForOutput(out);
      }
      catch(ScriptException e2)
      {   
        return null;
      }
    //}

  }

  public ByteString getScriptHashForAddress(String str)
  {
    Address addr = Address.fromString(params, str);
    Transaction tx= new Transaction(params);

    TransactionOutput tx_out = new TransactionOutput(params, tx, Coin.CENT, addr);

    return getScriptHashForOutput(tx_out);
  }

    /*public Address getAddressForOutput(TransactionOutput out)
    {
      try
      {
        Script script = out.getScriptPubKey();
        if (script.isSentToRawPubKey())
        {
          byte[] key = out.getScriptPubKey().getPubKey();
          byte[] address_bytes = org.bitcoinj.core.Utils.sha256hash160(key);
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

    }*/

    public HashSet<ByteString> getAllScriptHashes(Transaction tx, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
    {
        HashSet<ByteString> lst = new HashSet<ByteString>();

        for(TransactionInput in : tx.getInputs())
        {   
            ByteString a = getScriptHashForInput(in, confirmed, block_tx_map);
            if (a!=null) lst.add(a);
        }

        for(TransactionOutput out : tx.getOutputs())
        {   
            ByteString a = getScriptHashForOutput(out);
            if (a!=null) lst.add(a);
        }
        return lst;

    }


    /*public Address getAddressForInput(TransactionInput in, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
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
            int fail_count =0;
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
                  fail_count++;
                  if (fail_count > 30)
                  {
                    System.out.println("Unable to get source transaction: " + out_p.getHash());
                  }
                  if (fail_count > 240)
                  {
                    throw new RuntimeException("Waited too long to get transaction: " + out_p.getHash());
                  }
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

    }*/
    

  public int getTXBlockHeight(Transaction tx, BlockChainCache chain_cache, BitcoinRPC rpc)
  {
    Sha256Hash block_hash = rpc.getTransactionConfirmationBlock(tx.getHash());

    if (block_hash == null) return -1;
    return db.getBlockStoreMap().get(block_hash).getHeight();

  }

  /*public String getAddressFromPublicKeyHash(ByteString hash)
  {

    ByteString b160 = Util.RIPEMD160(hash);
    Address a = new Address(params, b160.toByteArray());
    System.out.println("Converted " + Util.getHexString(hash.toByteArray()) + " to " + a.toString());
    return a.toString();
  }*/

}
