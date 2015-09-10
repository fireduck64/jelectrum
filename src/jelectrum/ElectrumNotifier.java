
package jelectrum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Address;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.core.TransactionOutPoint;
import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.AddressFormatException;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Why is the logic of preparing results for clients
 * mixed between here and StratumConnection?  This needs to be refactored.
 */
public class ElectrumNotifier
{
    Map<String, Subscriber> block_subscribers;
    Map<String, Subscriber> blocknum_subscribers;
    Map<String, Map<String, Subscriber> > address_subscribers;
    LRUCache<String, String> address_sums;

    Jelectrum jelly;

    volatile StoredBlock chain_head;
    Object chain_head_lock= new Object();


    public ElectrumNotifier(Jelectrum jelly)
    {
        this.jelly = jelly;

        block_subscribers = new HashMap<String, Subscriber>(512, 0.5f);
        blocknum_subscribers = new HashMap<String, Subscriber>(512, 0.5f);
        address_subscribers = new HashMap<String, Map<String, Subscriber> >(512, 0.5f);
        address_sums = new LRUCache<String, String>(10000);

    }
    public void start()
        throws com.google.bitcoin.store.BlockStoreException
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
        try
        {
            blk = jelly.getBlockStore().get(b.getHash());
        }
        catch(com.google.bitcoin.store.BlockStoreException e)
        {
            throw new RuntimeException(e);
        }

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
    public void notifyNewTransaction(Transaction tx, Collection<String> addresses, int height)
    {
        synchronized(address_sums)
        {
            for(String s : addresses)
            {
                address_sums.remove(s);
            }
        }

        //Inside a sync do a deep copy of just the entries that we need
        Map<String, Map<String, Subscriber> > address_subscribers_copy = new HashMap<String, Map<String, Subscriber>>();
        synchronized(address_subscribers)
        {
            for(String s : addresses)
            {
                Map<String, Subscriber> m = address_subscribers.get(s);
                if (m != null)
                {
                    TreeMap<String, Subscriber> copy = new TreeMap<String, Subscriber>();
                    copy.putAll(m);
                    address_subscribers_copy.put(s, m);
                }
            }
        }



        //Now with our clean copy we can do the notifications without holding any locks
        try
        {
            for(String s : addresses)
            {
                Map<String, Subscriber> m = address_subscribers_copy.get(s);
                if ((m != null) && (m.size() > 0))
                {
                    String sum = getAddressChecksum(s); 

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
                block_data.put("utxo_root", jelly.getUtxoTrieMgr().getRootHash());



    }

    public void registerBlockchainAddress(StratumConnection conn, Object request_id, boolean send_initial, String address)
    {
        Subscriber sub = new Subscriber(conn, request_id);
        synchronized(address_subscribers)
        {
            if (address_subscribers.get(address) == null)
            {
                address_subscribers.put(address, new TreeMap<String, Subscriber>());
            }
            address_subscribers.get(address).put(conn.getId(), sub);
        }
        if (send_initial)
        {
            try
            {
                JSONObject reply = sub.startReply();
                String sum = getAddressChecksum(address);
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

    public void sendAddressHistory(StratumConnection conn, Object request_id, String address)
    {
        Subscriber sub = new Subscriber(conn, request_id);
        try
        {
            JSONObject reply = sub.startReply();

            reply.put("result", getAddressHistory(address));

            sub.sendReply(reply);


        }
        catch(org.json.JSONException e)
        {   
            throw new RuntimeException(e);
        }


    }
    public void sendUnspent(StratumConnection conn, Object request_id, String address)
      throws AddressFormatException
    {
      try
      {
        Subscriber sub = new Subscriber(conn, request_id);
        Address target = new Address(jelly.getNetworkParameters(), address);
        JSONObject reply = sub.startReply();

        Collection<TransactionOutPoint> outs = jelly.getUtxoTrieMgr().getUnspentForAddress(target);

        
        JSONArray arr =new JSONArray();


        for(TransactionOutPoint out : outs)
        {
          JSONObject o = new JSONObject();
          o.put("tx_hash", out.getHash().toString());
          o.put("tx_pos", out.getIndex());

          SortedTransaction s_tx = new SortedTransaction(out.getHash());

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

    public void sendAddressBalance(StratumConnection conn, Object request_id, String address)
      throws AddressFormatException
    {
      Address target = new Address(jelly.getNetworkParameters(), address);

      Subscriber sub = new Subscriber(conn, request_id);
        try
        {
            JSONObject reply = sub.startReply();

            List<SortedTransaction> lst = getTransactionsForAddress(address);

            TreeMap<String, Long> confirmed_outs = new TreeMap<>();
            TreeMap<String, Long> unconfirmed_outs = new TreeMap<>();


            // Add all outputs
            for(SortedTransaction stx : lst)
            {
              Transaction tx = stx.tx;
              int idx=0;
              for(TransactionOutput tx_out : tx.getOutputs())
              {
                Address a = jelly.getImporter().getAddressForOutput(tx_out);
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



    public Object getAddressHistory(String address)
    {
        try
        {
            List<SortedTransaction> lst = getTransactionsForAddress(address);
            if (lst.size() > 0)
            {
                JSONArray arr =new JSONArray();

                for(SortedTransaction ts : lst)
                {
                    JSONObject o = new JSONObject();
                    o.put("tx_hash", ts.tx.getHash().toString());
                    if (ts.block != null)
                    {
                        o.put("height", ts.height);
                    }
                    else
                    {
                        o.put("height", 0);
                    }
                    arr.put(o);
                }
                return arr;

            }
            else
            {
                return JSONObject.NULL;
            }
 
        }
        catch(org.json.JSONException e)
        {   
            throw new RuntimeException(e);
        }


    }

    public String getAddressChecksum(String address)
    {
        synchronized(address_sums)
        {
            if (address_sums.containsKey(address))
            {
                return address_sums.get(address);
            }
        }

        String hash = null;

        List<SortedTransaction> lst = getTransactionsForAddress(address);

        if (lst.size() > 0)
        {
            StringBuilder sb = new StringBuilder();
            for(SortedTransaction ts : lst)
            {
                sb.append(ts.tx.getHash());
                sb.append(':');
                if (ts.block != null)
                {
                    sb.append(ts.height);
                }
                else
                {
                    sb.append("0");
                }
                sb.append(':');
            }

            hash = Util.SHA256(sb.toString());
            
        }

        synchronized(address_sums)
        {
            address_sums.put(address,hash);
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
      synchronized(address_subscribers)
      {
        for(Map.Entry<String, Map<String, Subscriber>> me : address_subscribers.entrySet())
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

                synchronized(address_subscribers)
                {
                  for(Map.Entry<String, Map<String, Subscriber>> me : address_subscribers.entrySet())
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

    public List<SortedTransaction> getTransactionsForAddress(String address)
    {
        Set<Sha256Hash> tx_list = jelly.getDB().getAddressToTxSet(address);
        ArrayList<SortedTransaction> out = new ArrayList<SortedTransaction>();

        if (tx_list != null)
        {
            TreeSet<SortedTransaction> set = new TreeSet<SortedTransaction>();
            for(Sha256Hash tx_hash : tx_list)
            {
                SortedTransaction stx = new SortedTransaction(tx_hash);
                if (stx.isValid())
                {
                    set.add(stx);
                }
            }

            for(SortedTransaction s : set)
            {
                out.add(s);
            }



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
        StoredBlock block;
        int height;
        public SortedTransaction(Sha256Hash tx_hash)
        {
            this.s_tx = jelly.getDB().getTransactionMap().get(tx_hash);
            if (s_tx==null) return;
            this.tx = s_tx.getTx(jelly.getNetworkParameters());
            Set<Sha256Hash> block_list = jelly.getDB().getTxToBlockMap(tx.getHash());
            if (block_list != null)
            {
                
                for(Sha256Hash block_hash : block_list)
                {
                    block = jelly.getDB().getBlockStoreMap().get(block_hash);
                    height = block.getHeight();
                }

            }

        }

        public int getEffectiveHeight()
        {
            if (block != null) return height;
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
            if (block!=null) return true;
            if (s_tx.getSavedTime() + 86400L * 1000L > System.currentTimeMillis()) return true;
            return false;
        }

    }

    
}
