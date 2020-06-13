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
import java.util.ArrayList;
import java.util.HashSet;

import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.commons.codec.binary.Hex;
import java.nio.ByteBuffer;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Transaction;
import com.google.protobuf.ByteString;

public class StratumConnection
{
    //ghostbird, dirtnerd, beancurd, thingword, absurd, ferd, mort
    public static final String JELECTRUM_VERSION="mort";
    public static final String PROTO_VERSION="1.4";
    public static final boolean use_thread_per_request=false;

    public static ArrayList<String> SUPPORTED_PROTOS;
    {
      SUPPORTED_PROTOS = new ArrayList<>();
      SUPPORTED_PROTOS.add("1.4.2");
      SUPPORTED_PROTOS.add("1.4.1");
      SUPPORTED_PROTOS.add("1.4");
      SUPPORTED_PROTOS.add("1.1");
      SUPPORTED_PROTOS.add("0.10");
    }

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
    private String client_version;
    private String client_protocol="0.10";
    private AtomicInteger subscription_count = new AtomicInteger(0);
    private RateLimit session_rate_limit;

    private HashSet<Integer> chunks_downloaded;

    private LinkedBlockingQueue<JSONObject> out_queue = new LinkedBlockingQueue<JSONObject>();
    
    private long get_client_id=-1;

    public static final long PRINT_INFO_DELAY=15000L;

    private String banner="Jelectrum";

    private boolean detail_logs = false;


    public StratumConnection(Jelectrum jelectrum, StratumServer server, Socket sock, String connection_id)
      throws IOException
    {
      
        
        this.jelectrum = jelectrum;
        this.tx_util = jelectrum.getDB().getTXUtil();
        this.server = server;
        this.config = server.getConfig();
        this.sock = sock;
        this.connection_id = connection_id;

        detail_logs = jelectrum.getConfig().getBoolean("connection_detail_logs");

        open=true;

        chunks_downloaded = new HashSet<>();

        last_network_action=new AtomicLong(System.nanoTime());
        if (detail_logs)
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
        if (jelectrum.getConfig().isSet("session_rate_limit"))
        {
          session_rate_limit = new RateLimit(jelectrum.getConfig().getDouble("session_rate_limit"), 2.0);
        }
    
        new OutThread().start();
        new InThread().start();

    }

    public void close()
    {
        
        jelectrum.getEventLog().log("Closed called: " + sock + " " + connection_id);
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

    private void logRequest(String method, int input_size, int output_size)
    {
      if (detail_logs)
      {
        jelectrum.getEventLog().log(connection_id + " - " + method + " in: " + input_size + " out: " + output_size); 
      }
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
                        if (session_rate_limit != null)
                        {
                          session_rate_limit.waitForRate(msg_str.length());
                        }
                        server.applyGlobalRateLimit(msg_str.length());
                        out.println(msg_str);
                        out.flush();

                        //System.out.println("Out: " + msg.toString());
                        updateLastNetworkAction();
                    }


                    if (!info_printed)
                    if (open)
                    if (System.currentTimeMillis() > PRINT_INFO_DELAY + connection_start_time)
                    {
                      if (detail_logs)
                        jelectrum.getEventLog().log(connection_id + " - " + version_info + " " + subscription_count.get());
                        info_printed=true;
                    }
                }

            }
            catch(Throwable e)
            {
                jelectrum.getEventLog().log(connection_id + ": " + e);
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
          String line = null;
            try
            {
                Scanner scan = new Scanner(sock.getInputStream());

                while(open)
                {
                    line = scan.nextLine();
                    updateLastNetworkAction();
                    int input_size = line.length();
                    line = line.trim();
                    if (line.length() > 0)
                    {
                        JSONObject msg = new JSONObject(line);
                        if (use_thread_per_request)
                        {
                          new InWorkerThread(msg, input_size).start();
                        }
                        else
                        {

                          processInMessage(msg, input_size);
                        }
                    }

                }

            }
            catch(java.util.NoSuchElementException e)
            {
              if (detail_logs)  
                jelectrum.getEventLog().log("Connection closed " + sock + " " + connection_id);
            }
            catch(java.lang.NullPointerException npe)
            {
                jelectrum.getEventLog().log("Fireduck error (" + sock + " " +connection_id+"): " + npe);
                npe.printStackTrace();

            }
            catch(Throwable e)
            {
                jelectrum.getEventLog().log("Unexpected error (" + sock + " " +connection_id+"): " + e);
                jelectrum.getEventLog().log("Line: " + line);
            }
            finally
            {
                close();
            }

        }
    }

    public class InWorkerThread extends Thread
    {
      private JSONObject msg;
      private int input_size;
      public InWorkerThread(JSONObject msg, int input_size)
      {
        this.msg = msg;
        this.input_size = input_size;
        setName("InWorkerThread");
        setDaemon(true);
      }

      public void run()
      {
        try
        {
          processInMessage(msg, input_size);
        }
        catch(Throwable t)
        {
          if (detail_logs)
          {
            jelectrum.getEventLog().log(connection_id + ": error in processing: " + t);
          }
          close();
        }
      }
    }

    private void processInMessage(JSONObject msg, int input_size)
        throws Exception
    {
        long idx = msg.optLong("id",-1);
        Object id = msg.opt("id");
        JSONObject reply = new JSONObject();
        reply.put("id", id);
        reply.put("jsonrpc","2.0");
        try
        {
        
            if (!msg.has("method"))
            {
                jelectrum.getEventLog().alarm(connection_id + " - Unknown message: " + msg.toString());
                return;
            }
            String method = msg.getString("method");


            if (method.equals("server.version"))
            {
              jelectrum.getEventLog().log(String.format("%s - %s", connection_id, msg.toString()));
                  JSONArray version_array = msg.getJSONArray("params");

                  client_version = version_array.getString(0);
                  
                  if (version_array.optString(1) != null)
                  {
                    client_protocol = selectProto(version_array.optString(1), version_array.optString(1));
                    if (client_protocol == null) client_protocol = "0.10";

                  }
                  JSONArray proto_array = version_array.optJSONArray(1);
                  if (proto_array != null)
                  {   
                    String min_proto = proto_array.getString(0);
                    String max_proto = proto_array.getString(1);
                    client_protocol = selectProto(min_proto, max_proto);
                  }
                  if (client_protocol == null)
                  {
                    reply.put("error", "unsupported protocol");
                    jelectrum.getEventLog().log("Unable to agree on proto with: " + msg);
                  }
                  else
                  {
                    if (client_protocol.compareTo("1.1") >= 0)
                    {
                      JSONArray result_arr = new JSONArray();
                      result_arr.put(String.format("jelectum %s", JELECTRUM_VERSION));
                      result_arr.put(client_protocol);
                      reply.put("result", result_arr);
                    }
                    else
                    {
                      reply.put("result", client_protocol);
                    }
                  }

                  if (version_info == null)
                  {
                    //if (detail_logs)
                    jelectrum.getEventLog().log(String.format("%s - Version set from client (%s) using proto (%s)", connection_id, client_version, client_protocol));
                  }
                  
                version_info = msg.get("params").toString();

                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("server.ping"))
            {
              reply.put("result", JSONObject.NULL);
              logRequest(method, input_size, reply.toString().length());
              sendMessage(reply);
            }
            else if (method.equals("server.banner"))
            {
                reply.put("result",banner);
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("blockchain.headers.subscribe"))
            {
                jelectrum.getEventLog().log("Subscribe: " + msg);
                logRequest(method, input_size, 0);
                jelectrum.getElectrumNotifier().registerBlockchainHeaders(this, id, true);
            }
            else if ((method.equals("blockchain.numblocks.subscribe")) && (client_protocol.compareTo("1.1") < 0))
            {
                //Should send this on each new block:
                //{"id": 1, "result": {"nonce": 3114737334, "prev_block_hash": "000000000000000089e1f388af7cda336b6241b3f0b0ca36def7a8f22e44d39b", "timestamp": 1387995813, "merkle_root": "0debf5bd535624a955d229337a9bf3da5f370cc5a1f5fbee7261b0bdd0bd0f10", "block_height": 276921, "version": 2, "bits": 419668748}}

                logRequest(method, input_size, 0);
                jelectrum.getElectrumNotifier().registerBlockCount(this, id, true);
            }
            else if (method.equals("blockchain.address.get_history"))
            {
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                logRequest(method, input_size, 0);
                ByteString scripthash = tx_util.getScriptHashForAddress(address);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, scripthash, true, true);

            }
            else if (method.equals("blockchain.scripthash.get_history"))
            {
                JSONArray params = msg.getJSONArray("params");
                ByteString scripthash = ByteString.copyFrom(Hex.decodeHex(params.getString(0).toCharArray()));
                logRequest(method, input_size, 0);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, scripthash, true, true);
            }
            else if (method.equals("mempool.get_fee_histogram"))
            {

                reply.put("result", jelectrum.getMemPooler().getFeeHistogram());
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
              
            }
            
            else if (method.equals("blockchain.address.get_mempool"))
            {
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                logRequest(method, input_size, 0);
                ByteString scripthash = tx_util.getScriptHashForAddress(address);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, scripthash, false, true);
            }
            else if (method.equals("blockchain.scripthash.get_mempool"))
            {
                JSONArray params = msg.getJSONArray("params");
                ByteString scripthash = ByteString.copyFrom(Hex.decodeHex(params.getString(0).toCharArray()));
                logRequest(method, input_size, 0);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, scripthash, false, true);
            }
 
            else if ((method.equals("blockchain.address.get_proof")) && (client_protocol.compareTo("1.1") < 0))
            {
                
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                logRequest(method, input_size, 0);
                ByteString scripthash = tx_util.getScriptHashForAddress(address);
                jelectrum.getElectrumNotifier().sendAddressHistory(this, id, scripthash, true, true);

            }
            else if (method.equals("blockchain.address.get_balance"))
            {
                JSONArray params = msg.getJSONArray("params");
                String address = params.getString(0);
                ByteString scripthash = tx_util.getScriptHashForAddress(address);
                jelectrum.getElectrumNotifier().sendAddressBalance(this, id, scripthash);
            }
            else if (method.equals("blockchain.scripthash.get_balance"))
            {
                JSONArray params = msg.getJSONArray("params");
                ByteString scripthash = ByteString.copyFrom(Hex.decodeHex(params.getString(0).toCharArray()));
                jelectrum.getElectrumNotifier().sendAddressBalance(this, id, scripthash);
            }
            else if (method.equals("blockchain.address.listunspent"))
            {
              JSONArray params = msg.getJSONArray("params");
              logRequest(method, input_size, 0);

              String address = params.getString(0);
              ByteString scripthash = tx_util.getScriptHashForAddress(address);
              jelectrum.getElectrumNotifier().sendUnspent(this, id, scripthash);
            }
             else if (method.equals("blockchain.scripthash.listunspent"))
            {
              JSONArray params = msg.getJSONArray("params");
              logRequest(method, input_size, 0);
              ByteString scripthash = ByteString.copyFrom(Hex.decodeHex(params.getString(0).toCharArray()));
              jelectrum.getElectrumNotifier().sendUnspent(this, id, scripthash);
            }
            else if (method.equals("blockchain.address.subscribe"))
            {
                JSONArray params = msg.getJSONArray("params");

                logRequest(method, input_size, 0);

                String address = params.getString(0);
                ByteString scripthash = tx_util.getScriptHashForAddress(address);
                jelectrum.getElectrumNotifier().registerBlockchainAddress(this, id, true, scripthash);
                subscription_count.getAndIncrement();
            }
            else if (method.equals("blockchain.scripthash.subscribe"))
            {
                JSONArray params = msg.getJSONArray("params");
                logRequest(method, input_size, 0);

                ByteString scripthash = ByteString.copyFrom(Hex.decodeHex(params.getString(0).toCharArray()));
                subscription_count.getAndIncrement();

                jelectrum.getElectrumNotifier().registerBlockchainAddress(this, id, true, scripthash);
            }
 
            else if (method.equals("server.peers.subscribe"))
            {
                JSONArray lst = jelectrum.getPeerManager().getPeers();
                reply.put("result", lst);

                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("server.features"))
            {
                JSONObject data = jelectrum.getPeerManager().getServerFeatures();
                reply.put("result", data);

                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("server.add_peer"))
            {
                JSONArray params = msg.getJSONArray("params");
                jelectrum.getPeerManager().addPeers(params);

                reply.put("result", "OK");
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("server.donation_address"))
            {
             
              String addr = "";
              if (config.isSet("donation_address"))
              {
                addr = config.get("donation_address");
              }
              reply.put("result", addr);
              logRequest(method, input_size, reply.toString().length());

              sendMessage(reply);
            }
            else if (method.equals("blockchain.transaction.get"))
            {

                JSONArray params = msg.getJSONArray("params");

                {

                  Sha256Hash hash = null;
                  try
                  {
                    hash =Sha256Hash.wrap( params.getString(0));
                  }
                  catch(Throwable t)
                  {
                    throw new Exception("Bad transaction hash: " + params.getString(0));
                  }

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
                }
                logRequest(method, input_size, reply.toString().length());

                sendMessage(reply);
            }
            else if (method.equals("blockchain.block.get_header"))
            {
                 JSONArray arr = msg.getJSONArray("params");
                 int height = arr.getInt(0);

                 Sha256Hash block_hash = jelectrum.getBlockChainCache().getBlockHashAtHeight(height);
                 StoredBlock blk = jelectrum.getDB().getBlockStoreMap().get(block_hash);

                 JSONObject result = new JSONObject();
                 jelectrum.getElectrumNotifier().populateBlockData(blk, result);

                 reply.put("result", result);
                 logRequest(method, input_size, reply.toString().length());

                 sendMessage(reply);
            }
            else if (method.equals("blockchain.block.header"))
            {
                 JSONArray arr = msg.getJSONArray("params");
                 int height = arr.getInt(0);

                 Sha256Hash block_hash = jelectrum.getBlockChainCache().getBlockHashAtHeight(height);
                 StoredBlock blk = jelectrum.getDB().getBlockStoreMap().get(block_hash);


                 String header =  Util.getHeaderHex(blk.getHeader());
                 if (arr.length() == 1)
                 {
                    reply.put("result", header);
                 }
                 else
                 {

                  JSONObject result = new JSONObject();
                  result.put("header", header);
                  int cp_height = arr.getInt(1);


                  jelectrum.getBlockChainCache().populateBlockProof(block_hash, height, cp_height, result);


                  reply.put("result", result);

                 }

                 logRequest(method, input_size, reply.toString().length());

                 sendMessage(reply);
            }
            else if (method.equals("blockchain.block.headers"))
            {
                 JSONArray arr = msg.getJSONArray("params");
                 int height = arr.getInt(0);
                 int count = arr.getInt(1);
                 int cp = -1;

                 if (arr.length() == 3)
                 {
                    cp = arr.getInt(2);
                 }

                 reply.put("result", jelectrum.getHeaderChunkAgent().getHeaders(height, count, cp));

                 logRequest(method, input_size, reply.toString().length());

                 sendMessage(reply);
            }
 
            else if (method.equals("blockchain.transaction.broadcast"))
            {
                JSONArray arr = msg.getJSONArray("params");
                String hex = arr.getString(0);

                byte[] tx_data = new Hex().decode(hex.getBytes());
                Transaction tx = new Transaction(jelectrum.getNetworkParameters(), tx_data);

                if (jelectrum.getBitcoinRPC()!=null)
                {
                  jelectrum.getBitcoinRPC().submitTransaction(hex);
                }

                reply.put("result", tx.getHash().toString());
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);

                //jelectrum.getImporter().saveTransaction(tx);

            }
            else if (method.equals("blockchain.transaction.get_merkle"))
            {
                 JSONArray arr = msg.getJSONArray("params");
                 Sha256Hash tx_hash = Sha256Hash.wrap(arr.getString(0));
                 int height = arr.getInt(1);

                 Sha256Hash block_hash = jelectrum.getBlockChainCache().getBlockHashAtHeight(height);
                 Block blk = jelectrum.getDB().getBlock(block_hash).getBlock(jelectrum.getNetworkParameters());

                 JSONObject result =  Util.getMerkleTreeForTransaction(blk.getTransactions(), tx_hash);
                 result.put("block_height", height);

                 reply.put("result", result);

                 logRequest(method, input_size, reply.toString().length());
                 sendMessage(reply);
            }
            else if (method.equals("blockchain.block.get_chunk"))
            {
                JSONArray arr = msg.getJSONArray("params");
                int index = arr.getInt(0);

                if (chunks_downloaded.contains(index))
                {
                  reply.put("error","Chunk already downloaded");
                }
                else
                {
                  chunks_downloaded.add(index);

                  reply.put("result", jelectrum.getHeaderChunkAgent().getChunk(index));
                  logRequest(method, input_size, reply.toString().length());

                  sendMessage(reply);
                }
            }
            else if (method.equals("blockchain.estimatefee"))
            {
                JSONArray arr = msg.getJSONArray("params");
                int block = arr.getInt(0);

                double fee = -1;
                if (jelectrum.getBitcoinRPC() != null)
                {
                  fee = jelectrum.getBitcoinRPC().getFeeEstimate(block);
                }
                else
                {
                  fee = Util.getFeeEstimateMap().get(block);
                }

                reply.put("result", fee);
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
            else if (method.equals("blockchain.relayfee"))
            {
                double fee = 1e-05;
                /*if (jelectrum.getBitcoinRPC() != null)
                {
                  fee = jelectrum.getBitcoinRPC().getRelayFee();
                }*/

                reply.put("result", fee);
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
          
            else
            {
                jelectrum.getEventLog().alarm(connection_id + " - Unknown electrum method: " + method);
                System.out.println(msg);
                reply.put("error","unknown method - " + method);
                logRequest(method, input_size, reply.toString().length());
                sendMessage(reply);
            }
        }
        catch(Throwable t)
        {
                reply.put("error","Exception: " + t);
                sendMessage(reply);
                jelectrum.getEventLog().log(connection_id + " - error: " + t);
                jelectrum.getEventLog().log(t);
                if (detail_logs) {
                  t.printStackTrace();
                }
                close();
        }
    }


    public static String selectProto(String min, String max)
    {
      if (min.equals("1.0")) min = "0.10";

      for(String p : SUPPORTED_PROTOS)
      {
        if ((p.compareTo(max) <= 0) && (p.compareTo(min) >= 0)) return p;
      }

      return null;
    }
    

}
