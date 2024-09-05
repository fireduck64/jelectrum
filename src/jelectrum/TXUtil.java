package jelectrum;

import duckutil.TimeRecord;
import duckutil.TimeRecordAuto;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import jelectrum.db.DBFace;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Coin;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.bitcoinj.script.ScriptError;
import org.bitcoinj.script.ScriptException;
import static org.bitcoinj.script.ScriptOpCodes.*;
import snowblossom.lib.HexUtil;

public class TXUtil
{
 
  private DBFace db;
  private NetworkParameters params;
  private LRUCache<Sha256Hash, Transaction> transaction_cache;
  private Object tx_cache_lock = new Object();

  public static final int TX_CACHE_SIZE=64000;

  public static final boolean DEBUG_SCRIPT=false;

  public TXUtil(DBFace db, NetworkParameters params)
  {
    this.db = db;
    this.params = params;
  }

  public void saveTxCache(Block block)
  {

		HashMap<Sha256Hash, Transaction> m = new HashMap<>(512, 0.5f);

		for(Transaction tx : block.getTransactions())
		{
			m.put(tx.getHash(), SerializedTransaction.scrubTransaction(params,tx));
		}

		synchronized(tx_cache_lock)
		{
    	if (transaction_cache == null)
    	{
      	transaction_cache = new LRUCache<Sha256Hash, Transaction>(TX_CACHE_SIZE);
    	}
    	transaction_cache.putAll(m);
		}
  }

  public void saveTxCache(Transaction tx)
  {
    synchronized(tx_cache_lock)
    {
      if (transaction_cache == null)
      {
        transaction_cache = new LRUCache<Sha256Hash, Transaction>(TX_CACHE_SIZE);
      }
      transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params,tx));
    }
  }
  public void putTxCacheIfOpen(Transaction tx)
  {
    synchronized(tx_cache_lock)
    {
      if (transaction_cache != null)
      {
        transaction_cache.put(tx.getHash(), SerializedTransaction.scrubTransaction(params,tx));
      }
    }
  }

  public Transaction getTransaction(Sha256Hash hash)
  { 
    long t1 = System.nanoTime();
    Transaction tx = null;
    synchronized(tx_cache_lock)
    {
      if (transaction_cache != null) 
      {
        tx = transaction_cache.get(hash);
      }
    }
    if (tx != null)
    {
      TimeRecord.record(t1, "txutil_gettx_cache");
      return tx;
    }

    SerializedTransaction s_tx = db.getTransaction(hash);

    if (s_tx != null)
    { 
      tx = s_tx.getTx(params);
      putTxCacheIfOpen(tx);
      TimeRecord.record(t1, "txutil_gettx_db");
      return tx;
    }
    return null;
  }

  public ByteString getScriptHashForOutput(TransactionOutput out)
  {
    return Util.reverse(Util.SHA256BIN(ByteString.copyFrom(out.getScriptBytes())));
  }

  /**
   * There are some cases where this returns -1 rather than anything useful.
   */
  public long getValueForInput(TransactionInput in, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
  {
    if (in.isCoinBase()) throw new RuntimeException("coinbase");
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
            return -1L;
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
    return out.getValue().getValue();


  }


  public ByteString getScriptHashForInput(TransactionInput in, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
  {
    
    //System.out.println("In Script: " + Util.getHexString(ByteString.copyFrom(in.getScriptBytes())));
    if (in.isCoinBase()) return null;
    try (TimeRecordAuto tra = new TimeRecordAuto("txutil_parse_input"))
    {

      try
      {
        Address a = getFromAddress(in);
        new TimeRecordAuto("txutil_parse_input_getfromaddress").close();
        return getScriptHashForAddress(a.toString());
      }
      catch(ScriptException e)
      {
        //Lets try this the other way
        ByteString parsed_hash = null;

        parsed_hash = parseInputScript(in.getScriptSig());
        if (parsed_hash != null) return parsed_hash;

        parsed_hash = parseWitness(in);
        if (parsed_hash != null) return parsed_hash;

        try (TimeRecordAuto tra_out = new TimeRecordAuto("txutil_parse_input_output"))
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


          if (DEBUG_SCRIPT)
          {
            System.out.println("In unknown Script: " + in.getScriptSig() + " " + Util.getHexString(ByteString.copyFrom(in.  getScriptBytes())));
            if (in.hasWitness())
            {
              System.out.println("in witness: " + in. getWitness());
              System.out.println("witness pushes: " + in.getWitness().getPushCount());
              for(int i=0; i<in.getWitness().getPushCount(); i++)
              {

              }
            }
            System.out.println("Out unknown script: " + out.getScriptPubKey() + " " + Util.getHexString(ByteString.copyFrom(out.  getScriptBytes()))); 
          }
          ByteString out_pub = getScriptHashForOutput(out);

          return out_pub;


        }
        catch(ScriptException e2)
        {   
          return null;
        }
      }
    }

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

    public HashSet<ByteString> getAllOutputScriptHashes(Transaction tx, boolean confirmed, Map<Sha256Hash, Transaction> block_tx_map)
    {
        HashSet<ByteString> lst = new HashSet<ByteString>();

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

  // this is really not correct
  /*
  public String getAddressFromScriptHash(ByteString hash)
  {

    Address a = LegacyAddress.fromPubKeyHash(params, Util.RIPEMD160(hash).toByteArray());
    System.out.println("Converted " + Util.getHexString(hash.toByteArray()) + " to " + a.toString());
    return a.toString();
  }*/

  public static ByteString parseWitness(TransactionInput in)
  {
    if (in.hasWitness())
    if (in.getWitness().getPushCount() == 2)
    if (in.getWitness().getPush(1).length == 33)
    {
      try (TimeRecordAuto tra = new TimeRecordAuto("txutil_parse_input_witness"))
      {
        ByteString h = ByteString.copyFrom( in.getWitness().getPush(1));
        h = Util.SHA256BIN(h);
        h = Util.RIPEMD160(h);
        ByteString prefix = HexUtil.hexStringToBytes("0014");

        ByteString out_script = prefix.concat(h);

        return Util.reverse(Util.SHA256BIN(out_script));
      }
 
    }


    return null;
  }
  
  public static ByteString parseInputScript(Script script)
  {
    List<ScriptChunk> chunks = script.getChunks();


    if (chunks.size() == 1)
    if (chunks.get(0) != null)
    if (chunks.get(0).data != null)
    if (chunks.get(0).data.length == 22)
    {
      try (TimeRecordAuto tra = new TimeRecordAuto("txutil_parse_input_single"))
      {
        ByteString h = ByteString.copyFrom(chunks.get(0).data);
        h = Util.SHA256BIN(h);
        h = Util.RIPEMD160(h);
        ByteString prefix = HexUtil.hexStringToBytes("a914");
        ByteString suffix = HexUtil.hexStringToBytes("87");

        ByteString out_script = prefix.concat(h).concat(suffix);

        return Util.reverse(Util.SHA256BIN(out_script));
      }

    }

    return null;


  }


  public static byte[] getPubKey(TransactionInput in) throws ScriptException
  {
		Script script = in.getScriptSig();
		List<ScriptChunk> chunks = script.getChunks();

		// Copied from Bitcoinj release 0.14 Script.java getPubKey
    // Should handle old style things fine.  Probably.

        if (chunks.size() != 2) {
            throw new ScriptException(ScriptError.SCRIPT_ERR_UNKNOWN_ERROR, "Script not of right size, expecting 2 but got " + chunks.size());
        }
        final ScriptChunk chunk0 = chunks.get(0);
        final byte[] chunk0data = chunk0.data;
        final ScriptChunk chunk1 = chunks.get(1);
        final byte[] chunk1data = chunk1.data;
        if (chunk0data != null && chunk0data.length > 2 && chunk1data != null && chunk1data.length > 2) {
            // If we have two large constants assume the input to a pay-to-address output.
            return chunk1data;
        } else if (chunk1.equalsOpCode(OP_CHECKSIG) && chunk0data != null && chunk0data.length > 2) {
            // A large constant followed by an OP_CHECKSIG is the key.
            return chunk0data;
        } else {
            throw new ScriptException(ScriptError.SCRIPT_ERR_UNKNOWN_ERROR, "Script did not match expected form: " + script);
        }
  }

  public Address getFromAddress(TransactionInput in) throws ScriptException {

    return LegacyAddress.fromPubKeyHash(params, org.bitcoinj.core.Utils.sha256hash160(getPubKey(in)));
    //return new LegacyAddress(params, org.bitcoinj.core.Utils.sha256hash160(getPubKey(in)));
  }


}
