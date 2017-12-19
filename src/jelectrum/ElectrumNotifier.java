
package jelectrum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Collection;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.AddressFormatException;
import org.json.JSONObject;
import org.json.JSONArray;
import com.google.protobuf.ByteString;

import org.junit.Assert;
/**
 * Why is the logic of preparing results for clients
 * mixed between here and StratumConnection?  This needs to be refactored.
 */
public class ElectrumNotifier
{
    Map<String, Subscriber> block_subscribers;
    Map<String, Subscriber> blocknum_subscribers;
    Map<ByteString, Map<String, Subscriber> > scripthash_subscribers;
    LRUCache<ByteString, String> scripthash_sums;

    Jelectrum jelly;
    private TXUtil tx_util;

    volatile StoredBlock chain_head;
    Object chain_head_lock= new Object();


    public ElectrumNotifier(Jelectrum jelly)
    {
        this.jelly = jelly;
        tx_util = jelly.getDB().getTXUtil();

        block_subscribers = new HashMap<String, Subscriber>(512, 0.5f);
        blocknum_subscribers = new HashMap<String, Subscriber>(512, 0.5f);
        scripthash_subscribers = new HashMap<ByteString, Map<String, Subscriber> >(512, 0.5f);
        scripthash_sums = new LRUCache<ByteString, String>(10000);

    }
    public void start()
        throws org.bitcoinj.store.BlockStoreException
    {
        chain_head = jelly.getBlockStore().getChainHead();


        new PruneThread().start();

    }

    public int getHeadHeight()
    {
        return chain_head.getHeight();
    }


    public void registerBlockchainHeaders(StratumConnection conn, Object request_id, boolean send_initial)
    {
    
        Subscriber sub = new Subscriber(conn, request_id);
        synchronized(block_subscribers)
        {
            String conn_id = conn.getId();
            block_subscribers.put(conn_id, sub);
        }
        if (send_initial)
        {
            StoredBlock blk = chain_head;
            try
            {
                JSONObject reply = sub.startReply();

                JSONObject block_data = new JSONObject();
                populateBlockData(blk, block_data);
                reply.put("result", block_data);
                reply.put("jsonrpc", "2.0");

                sub.sendReply(reply);
            }
            catch(org.json.JSONException e)
            {
                throw new RuntimeException(e);
            }


        }

    }
    public void registerBlockCount(StratumConnection conn, Object request_id, boolean send_initial)
    {
    
        Subscriber sub = new Subscriber(conn, request_id);
        synchronized(blocknum_subscribers)
        {
            String conn_id = conn.getId();
            blocknum_subscribers.put(conn_id, sub);
        }
        if (send_initial)
        {
            StoredBlock blk = chain_head;
            try
            {
                JSONObject reply = sub.startReply();

                reply.put("result", blk.getHeight());
                reply.put("jsonrpc", "2.0");

                sub.sendReply(reply);
            }
            catch(org.json.JSONException e)
            {
                throw new RuntimeException(e);
            }

        }

    }

 
    public void notifyNewBlock(Block b)
    {
        if (chain_head == null) return;
        StoredBlock blk = null;
        blk = jelly.getBlockStore().get(b.getHash());

        synchronized(chain_head_lock)
        {
            if (blk.getHeight() >= chain_head.getHeight())
            {
                chain_head = blk;        
            }
        }
        synchronized(block_subscribers)
        {
            for(Subscriber sub : block_subscribers.values())
            {
                blockNotify(sub, chain_head);

            }
        }
        synchronized(blocknum_subscribers)
        {
            for(Subscriber sub : blocknum_subscribers.values())
            {
                blockNumNotify(sub, chain_head);

            }
        }
        
    }
    public void notifyNewTransaction(Collection<ByteString> scripthashes, int height)
    {
      Assert.assertNotNull(scripthashes);
        synchronized(scripthash_sums)
        {
            for(ByteString s : scripthashes)
            {
                scripthash_sums.remove(s);
            }
        }

        //Inside a sync do a deep copy of just the entries that we need
        Map<ByteString, Map<String, Subscriber> > scripthash_subscribers_copy = new HashMap<ByteString, Map<String, Subscriber>>();
        synchronized(scripthash_subscribers)
        {
            for(ByteString s : scripthashes)
            {
                Map<String, Subscriber> m = scripthash_subscribers.get(s);
                if (m != null)
                {
                    TreeMap<String, Subscriber> copy = new TreeMap<String, Subscriber>();
                    copy.putAll(m);
                    scripthash_subscribers_copy.put(s, m);
                }
            }
        }

        //Now with our clean copy we can do the notifications without holding any locks
        try
        {
            for(ByteString s : scripthashes)
            {
                Map<String, Subscriber> m = scripthash_subscribers_copy.get(s);
                if ((m != null) && (m.size() > 0))
                {
                    String sum = getScriptHashChecksum(s); 

                    JSONObject reply = new JSONObject();
                    JSONArray info = new JSONArray();
                    info.put(s);
                    info.put(sum);
                    reply.put("params", info);
                    reply.put("id", JSONObject.NULL);
                    reply.put("method", "blockchain.address.subscribe");


                    for(Subscriber sub : m.values())
                    {
                       sub.sendReply(reply);
                    }
                
                }
            }
        }
        catch(org.json.JSONException e)
        {
            throw new RuntimeException(e);
        }
        catch(jelectrum.db.DBTooManyResultsException e)
        {
          //LOL
        }

    }

    private void blockNotify(Subscriber sub, StoredBlock blk)
    {

        try
        {
                JSONObject reply = new JSONObject();

                JSONObject block_data = new JSONObject();
                populateBlockData(blk, block_data);

                JSONArray crap = new JSONArray();
                crap.put(block_data);

                reply.put("params", crap);
                reply.put("id", JSONObject.NULL);
                reply.put("method", "blockchain.headers.subscribe");

                sub.sendReply(reply);
      

        }
        catch(org.json.JSONException e)
        {
            throw new RuntimeException(e);
        }

    }
    private void blockNumNotify(Subscriber sub, StoredBlock blk)
    {

        try
        {
                JSONObject reply = new JSONObject();

                JSONArray crap = new JSONArray();
                crap.put(blk.getHeight());

                reply.put("params", crap);
                reply.put("id", JSONObject.NULL);
                reply.put("method", "blockchain.numblocks.subscribe");

                sub.sendReply(reply);
      

        }
        catch(org.json.JSONException e)
        {
            throw new RuntimeException(e);
        }

    }

 
    public void populateBlockData(StoredBlock blk, JSONObject block_data)
        throws org.json.JSONException
    {
        Block header = blk.getHeader();
        block_data.put("nonce", header.getNonce());
        block_data.put("prev_block_hash", header.getPrevBlockHash().toString());
        block_data.put("timestamp", header.getTimeSeconds());
        block_data.put("merkle_root", header.getMerkleRoot().toString());
        block_data.put("block_height", blk.getHeight());
        block_data.put("version",header.getVersion());
        block_data.put("bits", header.getDifficultyTarget());
        //block_data.put("utxo_root", jelly.getUtxoTrieMgr().getRootHash(header.getHash()));



    }

    public void registerBlockchainAddress(StratumConnection conn, Object request_id, boolean send_initial, ByteString scripthash)
    {
        Subscriber sub = new Subscriber(conn, request_id);
        synchronized(scripthash_subscribers)
        {
            if (scripthash_subscribers.get(scripthash) == null)
            {
                scripthash_subscribers.put(scripthash, new TreeMap<String, Subscriber>());
            }
            scripthash_subscribers.get(scripthash).put(conn.getId(), sub);
        }
        if (send_initial)
        {
            try
            {
                JSONObject reply = sub.startReply();
                String sum = getScriptHashChecksum(scripthash);
                if (sum==null)
                {
                    reply.put("result", JSONObject.NULL);
                }
                else
                {
                    reply.put("result", sum);
                }
                sub.sendReply(reply);
            }
            catch(org.json.JSONException e)
            {
                throw new RuntimeException(e);
            }
        }

    }

    public void sendAddressHistory(StratumConnection conn, Object request_id, ByteString scripthash, boolean include_confirmed, boolean include_mempool)
    {
        Subscriber sub = new Subscriber(conn, request_id);
        try
        {
            JSONObject reply = sub.startReply();

            reply.put("result", getScriptHashHistory(scripthash,include_confirmed,include_mempool));

            sub.sendReply(reply);


        }
        catch(org.json.JSONException e)
        {   
            throw new RuntimeException(e);
        }
    }

    public void sendUnspent(StratumConnection conn, Object request_id, ByteString target)
      throws AddressFormatException
    {
      try
      {
        Subscriber sub = new Subscriber(conn, request_id);
        JSONObject reply = sub.startReply();

        Collection<TransactionOutPoint> outs = jelly.getUtxoSource().getUnspentForScriptHash(target);

        
        JSONArray arr =new JSONArray();


        for(TransactionOutPoint out : outs)
        {
          JSONObject o = new JSONObject();
          o.put("tx_hash", out.getHash().toString());
          o.put("tx_pos", out.getIndex());

          SortedTransaction s_tx = new SortedTransaction(out.getHash(), false);

          Transaction tx = s_tx.tx; 
          long value = tx.getOutput((int)out.getIndex()).getValue().longValue();
          o.put("value",value);
          o.put("height", s_tx.getEffectiveHeight());
          
          arr.put(o);
        }


        reply.put("result", arr);



        sub.sendReply(reply);
      }
      catch(org.json.JSONException e)
      {   
        throw new RuntimeException(e);
      }
    }

    public void sendAddressBalance(StratumConnection conn, Object request_id, ByteString target)
      throws AddressFormatException
    {

      Subscriber sub = new Subscriber(conn, request_id);
        try
        {
            JSONObject reply = sub.startReply();

            List<SortedTransaction> lst = getTransactionsForScriptHash(target, true, true);

            TreeMap<String, Long> confirmed_outs = new TreeMap<>();
            TreeMap<String, Long> unconfirmed_outs = new TreeMap<>();


            // Add all outputs
            for(SortedTransaction stx : lst)
            {
              Transaction tx = stx.tx;
              int idx=0;
              for(TransactionOutput tx_out : tx.getOutputs())
              {
                Address a = tx_util.getAddressForOutput(tx_out);
                if (target.equals(a))
                {
                  String k = tx.getHash().toString() + ":" + idx;
                  if (stx.height > 0)
                  {
                    confirmed_outs.put(k, tx_out.getValue().longValue());
                  }
                  else
                  {
                    unconfirmed_outs.put(k, tx_out.getValue().longValue());
                  }
                }
                idx++;
              }
            }

            // Remove all inputs
            for(SortedTransaction stx : lst)
            {
              Transaction tx = stx.tx;
              boolean confirmed = (stx.height > 0);
              for(TransactionInput tx_in : tx.getInputs())
              {
                if (!tx_in.isCoinBase())
                {
                  TransactionOutPoint tx_op = tx_in.getOutpoint();
                  String k = tx_op.getHash().toString() + ":" + tx_op.getIndex();

                  if (confirmed)
                  {
                    confirmed_outs.remove(k);
                    unconfirmed_outs.remove(k);
                  }
                  else
                  {
                    unconfirmed_outs.remove(k);
                    if (confirmed_outs.containsKey(k))
                    {
                      unconfirmed_outs.put(k, -confirmed_outs.get(k));
                    }

                  }
                }
              }
              
            }

            long balance_confirmed=0;
            long balance_unconfirmed=0;
            for(long x : confirmed_outs.values()) balance_confirmed+=x;
            for(long x : unconfirmed_outs.values()) balance_unconfirmed+=x;


            JSONObject arr = new JSONObject();
            //JSONObject b_c = new JSONObject();
            //JSONObject b_u = new JSONObject();
            arr.put("confirmed", balance_confirmed);
            arr.put("unconfirmed", balance_unconfirmed);
            arr.put("transactions", lst.size());
            //arr.put(b_c);
            //arr.put(b_u);
            reply.put("result", arr);
            sub.sendReply(reply);


        }
        catch(org.json.JSONException e)
        {   
            throw new RuntimeException(e);
        }


    }



    public Object getScriptHashHistory(ByteString address, boolean include_confirmed, boolean include_mempool)
    {
        try
        {
            List<SortedTransaction> lst = getTransactionsForScriptHash(address,include_confirmed,include_mempool);
            //if (lst.size() > 0)
            {
                JSONArray arr =new JSONArray();

                for(SortedTransaction ts : lst)
                {
                    JSONObject o = new JSONObject();
                    o.put("tx_hash", ts.tx.getHash().toString());
                    if (ts.confirmed)
                    {
                        o.put("height", ts.height);
                    }
                    else
                    {
                        int height = 0;
                        if (jelly.getMemPooler().areSomeInputsPending(ts.tx)) height = -1;
                        o.put("height", height);
                    }
                    if (ts.fee >= 0)
                    {
                      o.put("fee",ts.fee);
                    }
                    arr.put(o);
                }
                return arr;

            }
            /*else
            {
                return JSONObject.NULL;
            }*/
 
        }
        catch(org.json.JSONException e)
        {   
            throw new RuntimeException(e);
        }


    }

    public String getScriptHashChecksum(ByteString address)
    {
        synchronized(scripthash_sums)
        {
            if (scripthash_sums.containsKey(address))
            {
                return scripthash_sums.get(address);
            }
        }

        String hash = null;

        List<SortedTransaction> lst = getTransactionsForScriptHash(address, true, true);

        if (lst.size() > 0)
        {
            StringBuilder sb = new StringBuilder();
            for(SortedTransaction ts : lst)
            {
                sb.append(ts.tx.getHash());
                sb.append(':');
                if (ts.confirmed)
                {
                    sb.append(ts.height);
                }
                else
                {
                    int height = 0;
                    if (jelly.getMemPooler().areSomeInputsPending(ts.tx)) height=-1;
                    sb.append(height);
                }
                sb.append(':');
            }

            hash = Util.SHA256(sb.toString());
            
        }

        synchronized(scripthash_sums)
        {
            scripthash_sums.put(address,hash);
        }
        return hash;


    }


    private void printSubscriptionSummary()
    {
      int conn_count = jelly.getStratumServer().getConnectionCount();
      int block_subscriber_count = 0;
      int blocknum_subscriber_count = 0;
      int address_subscriptions = 0;
      synchronized(block_subscribers)
      {
        block_subscriber_count = block_subscribers.size();
      }
      synchronized(blocknum_subscribers)
      {
        blocknum_subscriber_count = blocknum_subscribers.size();
      }
      synchronized(scripthash_subscribers)
      {
        for(Map.Entry<ByteString, Map<String, Subscriber>> me : scripthash_subscribers.entrySet())
        {
          address_subscriptions += me.getValue().size();
        }

      }

      jelly.getEventLog().log("USERS Connections: " + conn_count + " Block subs: " + block_subscriber_count + " Block num subs: " + blocknum_subscriber_count + " Address subs: " + address_subscriptions);

    
    }

    public class PruneThread extends Thread
    {
        public PruneThread()
        {
            setName("ElectrumNotifier/PruneThread");
            setDaemon(true);
        }
        public void run()
        {
            while(true)
            {
                try{Thread.sleep(60000);}catch(Exception e){}


                synchronized(block_subscribers)
                {
                  pruneMap(block_subscribers);
                }
                synchronized(blocknum_subscribers)
                {
                  pruneMap(blocknum_subscribers);
                }

                synchronized(scripthash_subscribers)
                {
                  for(Map.Entry<ByteString, Map<String, Subscriber>> me : scripthash_subscribers.entrySet())
                  {
                    pruneMap(me.getValue());
                  }
                }
                printSubscriptionSummary();
            }


        }
        private void pruneMap(Map<String, Subscriber> input_map)
        {
          TreeSet<String> to_delete =new TreeSet<String>();
          for(Subscriber sub : input_map.values())
          {
            if (!sub.isOpen())
            {
              to_delete.add(sub.getId());
            }
          }
          for(String id : to_delete)
          {
            input_map.remove(id);
          }
        }
    }

    public List<SortedTransaction> getTransactionsForScriptHash(ByteString scripthash, boolean include_confirmed, boolean include_mempool)
    {

      TreeSet<SortedTransaction> set = new TreeSet<SortedTransaction>();

      if (include_confirmed)
      {

        Set<Sha256Hash> tx_list = jelly.getDB().getScriptHashToTxSet(scripthash);
        if (tx_list != null)
        {
            for(Sha256Hash tx_hash : tx_list)
            {
                SortedTransaction stx = new SortedTransaction(tx_hash, false);
                if (stx.isValid())
                {
                    set.add(stx);
                }
            }
        }
      }

      if (include_mempool)
      {
        Set<Sha256Hash> tx_mem_list = jelly.getMemPooler().getTxForScriptHash(scripthash);
        for(Sha256Hash tx_hash : tx_mem_list)
        {
          SortedTransaction stx = new SortedTransaction(tx_hash, true);
          if (stx.isValid())
          {
            set.add(stx);
          }
        }
      }
      
      ArrayList<SortedTransaction> out = new ArrayList<SortedTransaction>();
      for(SortedTransaction s : set)
      {
        out.add(s);
      }
      return out;
 
    }

    public class Subscriber
    {
        private StratumConnection conn;
        private Object request_id;

        public Subscriber(StratumConnection conn, Object request_id)
        {
            this.conn = conn;
            this.request_id = request_id;
        }

        public JSONObject startReply()
            throws org.json.JSONException
        {
            JSONObject reply = new JSONObject();
            reply.put("id", request_id);
            reply.put("jsonrpc", "2.0");

            return reply;

        }
        public void sendReply(JSONObject o)
        {
            conn.sendMessage(o);
        }
        public boolean isOpen()
        {
            return conn.isOpen();
        }
        public String getId()
        {
            return conn.getId();
        }
    }

    public class SortedTransaction implements Comparable<SortedTransaction>
    {
        SerializedTransaction s_tx;
        Transaction tx;
        boolean confirmed;
        boolean mempool;
        int height;
        long fee=-1;

        public SortedTransaction(Sha256Hash tx_hash, boolean mempool)
        {
          this.mempool = mempool;

          this.s_tx = jelly.getDB().getTransaction(tx_hash);
          if (s_tx==null) return;
          this.tx = s_tx.getTx(jelly.getNetworkParameters());
          
          height = tx_util.getTXBlockHeight(tx, jelly.getBlockChainCache(), jelly.getBitcoinRPC());
          if (height >= 0) confirmed=true;

          if (tx!=null)
          {
            if (tx.getFee() != null)
            {
              this.fee = tx.getFee().getValue();
            }
          }

        }

        public int getEffectiveHeight()
        {
            if (confirmed) return height;
            return Integer.MAX_VALUE;
        }

        public int compareTo(SortedTransaction o)
        {
            if (getEffectiveHeight() < o.getEffectiveHeight()) return -1;
            if (getEffectiveHeight() > o.getEffectiveHeight()) return 1;

            return tx.getHash().toString().compareTo(o.tx.getHash().toString());

        }
        public boolean isValid()
        {
            if (s_tx ==null) return false;
            if (confirmed) return true;
            if (mempool) return true;

            // These days, it must be from a block or from the mempool
            // so almost certainly valid

            /*if (s_tx.getSavedTime() + 86400L * 1000L < System.currentTimeMillis()) return false;

            // For unconfirmed transactions, make sure all the inputs
            // are known
            for(TransactionInput tx_in : s_tx.getTx(jelly.getNetworkParameters()).getInputs())
            {
              TransactionOutPoint op = tx_in.getOutpoint();
              Sha256Hash tx_in_h = op.getHash();
              SerializedTransaction s_in_tx = jelly.getDB().getTransaction(tx_in_h);
              if (s_in_tx == null) return false;
            }*/

            return false;
        }

    }

    
}
