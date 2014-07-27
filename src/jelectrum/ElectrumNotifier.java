
package jelectrum;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
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

    StoredBlock chain_head;
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
            if (blk.getHeight() > chain_head.getHeight())
            {
                chain_head = blk;        
            }
        }
        synchronized(block_subscribers)
        {
            for(Subscriber sub : block_subscribers.values())
            {
                blockNotify(sub, blk);

            }
        }
        synchronized(blocknum_subscribers)
        {
            for(Subscriber sub : blocknum_subscribers.values())
            {
                blockNumNotify(sub, blk);

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
                block_data.put("utxo_root", Util.SHA256(header.getHash().toString()));



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

                TreeSet<String> to_delete =new TreeSet<String>();

                synchronized(block_subscribers)
                {
                    for(Subscriber sub : block_subscribers.values())
                    {
                        if (!sub.isOpen())
                        {
                            to_delete.add(sub.getId());
                        }
                    }

                    for(String id : to_delete)
                    {
                        block_subscribers.remove(id);
                    }
                }
                //TODO - finish this monster
                /*synchronized(address_subscribers)
                {
                    for(String address : address_subscribers.keySet())
                    {
                        
                    }
                }*/
            }


        }
    }

    public List<SortedTransaction> getTransactionsForAddress(String address)
    {
        HashSet<Sha256Hash> tx_list = jelly.getDB().getAddressToTxMap().get(address);
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
            HashSet<Sha256Hash> block_list = jelly.getDB().getTxToBlockMap().get(tx.getHash());
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
            /*if (getEffectiveHeight() > o.getEffectiveHeight()) return -1;
            if (getEffectiveHeight() < o.getEffectiveHeight()) return 1;*/

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
