package jelectrum;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.IOException;

import java.io.PrintStream;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Scanner;
import java.util.Random;

import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.commons.codec.binary.Hex;
import java.nio.ByteBuffer;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Transaction;
import org.apache.commons.codec.binary.Hex;

public class StratumConnection
{
  //ghostbird, dirtnerd, beancurd
    public static final String JELECTRUM_VERSION="beancurd";
    public static final String PROTO_VERSION="1.0";

    private Jelectrum jelectrum;
    private StratumServer server;
    private Socket sock;
    private String connection_id;
    private AtomicLong last_network_action;
    private volatile boolean open;
    private Config config;
    private TXUtil tx_util;

    private long connection_start_time;
    private String version_info;
    private String first_address;
    private AtomicInteger subscription_count = new AtomicInteger(0);
   

    private LinkedBlockingQueue<JSONObject> out_queue = new LinkedBlockingQueue<JSONObject>();
    
    private long get_client_id=-1;
    private String client_version;

    public static final long PRINT_INFO_DELAY=15000L;

    private String banner="Jelectrum";


    public StratumConnection(Jelectrum jelectrum, StratumServer server, Socket sock, String connection_id)
      throws IOException
    {
        
        this.jelectrum = jelectrum;
        this.tx_util = new TXUtil(jelectrum.getDB(), jelectrum.getNetworkParameters());
        this.server = server;
        this.config = server.getConfig();
        this.sock = sock;
        this.connection_id = connection_id;

        open=true;

        last_network_action=new AtomicLong(System.nanoTime());

        jelectrum.getEventLog().log("New connection from: " + sock + " " + connection_id);
        connection_start_time = System.currentTimeMillis();

        if (jelectrum.getConfig().get("banner_file") != null)
        {
          String banner_file_path = jelectrum.getConfig().get("banner_file");
          DataInputStream d_in = new DataInputStream(new FileInputStream(banner_file_path));

          int len = (int)new File(banner_file_path).length();
          byte b[] = new byte[len];
          d_in.readFully(b);
          banner = new String(b);
          d_in.close();

        }
    
        new OutThread().start();
        new InThread().start();

    }

    public void close()
    {
        open=false;
        try
        {
            sock.close();
        }
        catch(Throwable t){}
    }

    public boolean isOpen()
    {
        return open;
    }   

    public long getLastNetworkAction()
    {
        return last_network_action.get();
    }

    protected void updateLastNetworkAction()
    {
        last_network_action.set(System.nanoTime());
    }

    public String getId()
    {
        return connection_id;
    }

    public void sendMessage(JSONObject msg)
    {
        try
        {
            out_queue.put(msg); 
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }



    public class OutThread extends Thread
    {
        public OutThread()
        {
            setName("OutThread");
            setDaemon(true);
        }

        public void run()
        {
            boolean info_printed=false;
            try
            {
                PrintStream out = new PrintStream(sock.getOutputStream());
                while(open)
                {
                    //Using poll rather than take so this thread will
                    //exit if the connection is closed.  Otherwise,
                    //it would wait forever on this queue
                    JSONObject msg = out_queue.poll(30, TimeUnit.SECONDS);
                    if (msg != null)
                    {

                        String msg_str = msg.toString(0);
                        out.println(msg_str);
                        out.flush();

                        //System.out.println("Out: " + msg.toString());
                        updateLastNetworkAction();
                    }


                    if (!info_printed)
                    if (open)
                    if (first_address != null)
                    if (System.currentTimeMillis() > PRINT_INFO_DELAY + connection_start_time)
                    {
                        jelectrum.getEventLog().log(connection_id + " - " + version_info + " " + subscription_count.get());
                        info_printed=true;
                    }
                }

            }
            catch(Throwable e)
            {
                System.out.println(connection_id + ": " + e);
            }
            finally
            {
                close();
            }

        }
    }
    public class InThread extends Thread
    {
        public InThread()
        {
            setName("InThread");
            setDaemon(true);
        }

        public void run()
        {
            try
            {
                Scanner scan = new Scanner(sock.getInputStream());

                while(open)
                {
                    String line = scan.nextLine();
                    updateLastNetworkAction();
                    line = line.trim();
                    if (line.length() > 0)
                    {
                        JSONObject msg = new JSONObject(line);
                        //System.out.println("In: " + msg.toString());
                        processInMessage(msg);
                    }

                }

            }
            catch(java.util.NoSuchElementException e)
            {
                jelectrum.getEventLog().log("Connection closed " + sock + " " + connection_id);
            }
            catch(Throwable e)
            {
                jelectrum.getEventLog().log("Unexpected error ("+connection_id+"): " + e);
            }
            finally
            {
                close();
            }

        }
    }

    private void processInMessage(JSONObject msg)
        throws Exception
    {
        long idx = msg.optLong("id",-1);
        Object id = msg.opt("id");
        try
        {
        
            if (!msg.has("method"))
            {
                System.out.println("Unknown message: " + msg.toString());
                return;
            }
            String method = msg.getString("method");
            if (method.equals("server.version"))
            {
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                reply.put("result",PROTO_VERSION);
                reply.put("jelectrum",JELECTRUM_VERSION);
                version_info = msg.get("params").toString();
                sendMessage(reply);
            }
            else if (method.equals("server.banner"))
            {
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                reply.put("result",banner);
                sendMessage(reply);
            }
            else if (method.equals("blockchain.headers.subscribe"))
            {
                //Should send this on each new block:
                //{"id": 1, "result": {"nonce": 3114737334, "prev_block_hash": "000000000000000089e1f388af7cda336b6241b3f0b0ca36def7a8f22e44d39b", "timestamp": 1387995813, "merkle_root": "0debf5bd535624a955d229337a9bf3da5f370cc5a1f5fbee7261b0bdd0bd0f10", "block_height": 276921, "version": 2, "bits": 419668748}}

                jelectrum.getElectrumNotifier().registerBlockchainHeaders(this, id, true);
                
            }
            else if (method.equals("blockchain.numblocks.subscribe"))
            {
                //Should send this on each new block:
                //{"id": 1, "result": {"nonce": 3114737334, "prev_block_hash": "000000000000000089e1f388af7cda336b6241b3f0b0ca36def7a8f22e44d39b", "timestamp": 1387995813, "merkle_root": "0debf5bd535624a955d229337a9bf3da5f370cc5a1f5fbee7261b0bdd0bd0f10", "block_height": 276921, "version": 2, "bits": 419668748}}

                jelectrum.getElectrumNotifier().registerBlockCount(this, id, true);
                
            }
            else if (method.equals("blockchain.address.get_history"))
            {
                //{"id": 29, "result": [{"tx_hash": "fc054ede2383904323d9b54991693b9150bb1a0a7cd3c344afb883d3ffc093f4", "height": 274759}, {"tx_hash": "9dc9363fe032e08630057edb61488fc8fa9910d8b21f02eb1b12ef2928c88550", "height": 274709}]}
                
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, address);

            }
            else if (method.equals("blockchain.address.get_proof"))
            {
                
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, address);

            }
 
            else if (method.equals("blockchain.address.get_balance"))
            {
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                jelectrum.getElectrumNotifier().sendAddressBalance(this, id, address);
            }
            else if (method.equals("blockchain.address.listunspent"))
            {
              JSONArray params = msg.getJSONArray("params");
              String address = params.getString(0);
              jelectrum.getElectrumNotifier().sendUnspent(this, id, address);
            }
            else if (method.equals("blockchain.address.subscribe"))
            {
                //the result is
                //sha256(fc054ede2383904323d9b54991693b9150bb1a0a7cd3c344afb883d3ffc093f4:274759:7bb11e62ceb5c9e918d9de541ec8d5d215353c6bbf2fcb32b300ec641f3a0b3f:274708:)
                //tx:height:tx:height:
                //or null if no transactions

                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);

                subscription_count.getAndIncrement();
                if (first_address==null)
                {
                    first_address=address;
                }

                jelectrum.getElectrumNotifier().registerBlockchainAddress(this, id, true, address);

            }

            else if (method.equals("server.peers.subscribe"))
            {
                JSONObject reply = new JSONObject();
                JSONArray lst = new JSONArray();
                reply.put("id", id);
                reply.put("result", lst);
                sendMessage(reply);
            }
            else if (method.equals("blockchain.transaction.get"))
            {
                JSONObject reply = new JSONObject();
                reply.put("id", id);

                JSONArray params = msg.getJSONArray("params");

                Sha256Hash hash =new Sha256Hash(params.getString(0));

                Transaction tx = tx_util.getTransaction(hash);
                if (tx==null)
                {
                    reply.put("error","unknown transaction");
                }
                else
                {
                    byte buff[]= tx.bitcoinSerialize();
                    reply.put("result", Util.getHexString(buff));
                }

                sendMessage(reply);
            }
            else if (method.equals("blockchain.block.get_header"))
            {
                 JSONObject reply = new JSONObject();
                 reply.put("id", id);

                 JSONArray arr = msg.getJSONArray("params");
                 int height = arr.getInt(0);

                 Sha256Hash block_hash = jelectrum.getBlockChainCache().getBlockHashAtHeight(height);
                 StoredBlock blk = jelectrum.getDB().getBlockStoreMap().get(block_hash);

                 JSONObject result = new JSONObject();
                 jelectrum.getElectrumNotifier().populateBlockData(blk, result);

                 reply.put("result", result);

                 sendMessage(reply);
     
            }
            else if (method.equals("blockchain.transaction.broadcast"))
            {
                JSONArray arr = msg.getJSONArray("params");
                String hex = arr.getString(0);

                byte[] tx_data = new Hex().decode(hex.getBytes());
                Transaction tx = new Transaction(jelectrum.getNetworkParameters(), tx_data);

                //jelectrum.getPeerGroup().broadcastTransaction(tx);
                JSONObject res = jelectrum.getBitcoinRPC().submitTransaction(hex);


                JSONObject reply = new JSONObject();

                reply.put("id", id);
                reply.put("result", tx.getHash().toString());
                sendMessage(reply);

                jelectrum.getImporter().saveTransaction(tx);


            }
            else if (method.equals("blockchain.transaction.get_merkle"))
            {
                 JSONObject reply = new JSONObject();
                 reply.put("id", id);

                 JSONArray arr = msg.getJSONArray("params");
                 Sha256Hash tx_hash = new Sha256Hash(arr.getString(0));
                 int height = arr.getInt(1);

                 Sha256Hash block_hash = jelectrum.getBlockChainCache().getBlockHashAtHeight(height);
                 Block blk = jelectrum.getDB().getBlockMap().get(block_hash).getBlock(jelectrum.getNetworkParameters());

                 JSONObject result =  Util.getMerkleTreeForTransaction(blk.getTransactions(), tx_hash);
                 result.put("block_height", height);

                 reply.put("result", result);

                 sendMessage(reply);
            }
            else if (method.equals("blockchain.block.get_chunk"))
            {
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                JSONArray arr = msg.getJSONArray("params");
                int index = arr.getInt(0);

                reply.put("result", jelectrum.getHeaderChunkAgent().getChunk(index));

                sendMessage(reply);
            }
            else if (method.equals("blockchain.estimatefee"))
            {
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                JSONArray arr = msg.getJSONArray("params");
                int block = arr.getInt(0);

                reply.put("result", jelectrum.getBitcoinRPC().getFeeEstimate(block));
                sendMessage(reply);
            }
            else
            {
                jelectrum.getEventLog().alarm(connection_id + " - Unknown electrum method: " + method);
                System.out.println(msg);
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                reply.put("error","unknown method - " + method);
                sendMessage(reply);
            }
        }
        catch(Throwable t)
        {
                JSONObject reply = new JSONObject();
                reply.put("id", id);
                reply.put("error","Exception: " + t);
                sendMessage(reply);
                jelectrum.getEventLog().log(connection_id + " - error: " + t);
                jelectrum.getEventLog().log(t);
                //t.printStackTrace();
        }
    }

}
