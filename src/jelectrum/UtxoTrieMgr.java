
package jelectrum;

import java.util.Collection;
import java.util.LinkedList;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Set;
import java.util.List;

import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.net.URL;

import com.google.bitcoin.core.Block;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.TransactionInput;
import com.google.bitcoin.core.TransactionOutput;
import com.google.bitcoin.core.TransactionOutPoint;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.NetworkParameters;
import com.google.bitcoin.core.ScriptException;
import com.google.bitcoin.core.Address;
import com.google.bitcoin.script.Script;
import com.google.bitcoin.script.ScriptChunk;

import java.io.FileInputStream;
import java.util.Scanner;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.io.FileOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Hex;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Random;

import java.util.concurrent.LinkedBlockingQueue;

import static com.google.bitcoin.script.ScriptOpCodes.*;

/**
 * Blocks have to be loaded in order
 * 
 * @todo - Add shutdown hook to avoid exit during flush
 * 
 */
public class UtxoTrieMgr
{
  // A key is 20 bytes of public key, 32 bytes of transaction id and 4 bytes of output index
  public static final int ADDR_SPACE = 56;
  public static boolean DEBUG=false;

  private NetworkParameters params;
  private Jelectrum jelly;
  private TXUtil tx_util;

  private StatData get_block_stat=new StatData();
  private StatData add_block_stat=new StatData();
  private StatData get_hash_stat=new StatData();
 
  // Maps a partial key prefix to a tree node
  // The tree node has children, which are other prefixes or full keys
  protected TreeMap<String, UtxoTrieNode> node_map = new TreeMap<String, UtxoTrieNode>();

  protected Map<String, UtxoTrieNode> db_map;

  protected Sha256Hash last_flush_block_hash;
  protected Sha256Hash last_added_block_hash;

  protected Object block_notify= new Object();
  protected Object block_done_notify = new Object();

  //Not to be trusted, only used for logging
  protected int block_height;

  protected volatile boolean caught_up=false;

  protected static PrintStream debug_out;

  public UtxoTrieMgr(Jelectrum jelly)
    throws java.io.FileNotFoundException
  {
    this.jelly = jelly;
    this.params = jelly.getNetworkParameters();

    tx_util = new TXUtil(jelly.getDB(), params);


    db_map = jelly.getDB().getUtxoTrieMap();



    if (jelly.getConfig().isSet("utxo_reset") && jelly.getConfig().getBoolean("utxo_reset"))
    {
      jelly.getEventLog().alarm("UTXO reset");
      resetEverything();
    }

    if (DEBUG)
    {
      
      debug_out = new PrintStream(new FileOutputStream("utxo-debug.log"));
    }


  }

  public boolean isUpToDate()
  {
    return caught_up;
  }

  private boolean started=false;

  public void start()
  {
    if (started) return;
    started=true;

    new UtxoMgrThread().start();
    new UtxoCheckThread().start();

  }

  public void resetEverything()
  {
    node_map.clear();
    putSaveSet("", new UtxoTrieNode(""));
    last_flush_block_hash = params.getGenesisBlock().getHash();
    last_added_block_hash = params.getGenesisBlock().getHash();
    flush();

  }

  private void flush()
  {
    DecimalFormat df = new DecimalFormat("0.000");

    //get_block_stat.print("get_block", df);
    //add_block_stat.print("add_block", df);
    //get_hash_stat.print("get_hash", df);
    jelly.getEventLog().alarm("UTXO Flushing: " + node_map.size() + " height: " + block_height);
    saveState(new UtxoStatus(last_added_block_hash, last_flush_block_hash));
    db_map.putAll(node_map.descendingMap());
    node_map.clear();
    saveState(new UtxoStatus(last_added_block_hash));
    last_flush_block_hash = last_added_block_hash;
    jelly.getEventLog().alarm("UTXO Flush complete");
  }

  public void saveState(UtxoStatus status)
  {
    jelly.getDB().getSpecialObjectMap().put("utxo_trie_mgr_state", status);
  }
  public synchronized UtxoStatus getUtxoState()
  {
    try
    {
      Object o = jelly.getDB().getSpecialObjectMap().get("utxo_trie_mgr_state");
      if (o != null)
      {
        return (UtxoStatus)o;
      }
    }
    catch(Throwable t)
    {
      t.printStackTrace();
    }

    jelly.getEventLog().alarm("Problem loading UTXO status, starting fresh");
    resetEverything();
    return getUtxoState();
  }


  public synchronized void addBlock(Block b)
  {
    for(Transaction tx : b.getTransactions())
    {
      addTransaction(tx);
    }
    last_added_block_hash = b.getHash();

  }


  // They see me rollin, they hating
  public synchronized void rollbackBlock(Block b)
  {
    LinkedList<Transaction> back_list = new LinkedList<Transaction>();

    for(Transaction tx : b.getTransactions())
    {
      back_list.addFirst(tx);
    }

    for(Transaction tx : back_list)
    {
      rollTransaction(tx);
    }

  }

  public void putSaveSet(String prefix, UtxoTrieNode node)
  {
    node_map.put(prefix, node);
  }

  public UtxoTrieNode getByKey(String prefix)
  {
    UtxoTrieNode n = node_map.get(prefix);
    if (n != null) return n;

    n = db_map.get(prefix);
    if (n != null) return n;



    jelly.getEventLog().alarm("UTXO node missing: ." + prefix + ".");


    return n;
  }

  private void checkUtxoHash(int height, Sha256Hash block, Sha256Hash utxo_hash)
  {
    UtxoCheckEntry check_entry = new UtxoCheckEntry(height, block, utxo_hash);

    check_queue.offer(check_entry);

  }

  private void addTransaction(Transaction tx)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      String key = getKeyForOutput(tx_out, idx);
      if (DEBUG) debug_out.println("Adding key: " + key);
      if (key != null)
      {
        getByKey("").addHash(key, this);

      }
      idx++;
    }

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        String key = getKeyForInput(tx_in);
        if (key != null)
        {
          getByKey("").removeHash(key, this);
        }
      }
    }
  }
  

  private void rollTransaction(Transaction tx)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      String key = getKeyForOutput(tx_out, idx);
      if (DEBUG) System.out.println("Adding key: " + key);
      if (key != null)
      {
        getByKey("").removeHash(key, this);

      }
      idx++;
    }

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        String key = getKeyForInput(tx_in);
        if (key != null)
        {
          getByKey("").addHash(key, this);
        }
      }
    }
  }

  public synchronized Collection<TransactionOutPoint> getUnspentForAddress(Address a)
  { 
    String prefix = getPrefixForAddress(a);

    Collection<String> keys = getByKey("").getKeysByPrefix(prefix, this);

    LinkedList<TransactionOutPoint> outs = new LinkedList<TransactionOutPoint>();

    try
    {
      for(String key : keys)
      {
        byte[] key_data=Hex.decodeHex(key.toCharArray());
        ByteBuffer bb = ByteBuffer.wrap(key_data);
        bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        bb.position(20);
        byte[] tx_data = new byte[32];
        bb.get(tx_data);
        int idx=bb.getInt();
        Sha256Hash tx_id = new Sha256Hash(tx_data);
        TransactionOutPoint o = new TransactionOutPoint(jelly.getNetworkParameters(), idx, tx_id);
        outs.add(o);
       
      }
    }
    catch(org.apache.commons.codec.DecoderException e)
    { 
      throw new RuntimeException(e);
    }



    return outs;
  }


  public synchronized Sha256Hash getRootHash()
  {
    Sha256Hash root = getByKey("").getHash("", this);
    if (DEBUG) debug_out.println("Root is now: " + root);
    return root;

  }
  
  public static int commonLength(String a, String b)
  {
    int max = Math.min(a.length(), b.length());

    int same = 0;
    for(int i=0; i<max; i++)
    {
      if (a.charAt(i) == b.charAt(i)) same++;
      else break;
    }
    if (same % 2 == 1) same--;
    return same;

  }

  public static Sha256Hash hashThings(String skip, Collection<Sha256Hash> hash_list)
  {
    try
    {  
      
      if (DEBUG) debug_out.print("hash(");

      MessageDigest md = MessageDigest.getInstance("SHA-256");
      if ((skip != null) && (skip.length() > 0))
      {
        byte[] skipb = Hex.decodeHex(skip.toCharArray());
        md.update(skipb);
        if (DEBUG) 
        {
          debug_out.print("skip:");
          debug_out.print(skip);
          debug_out.print(' ');
        }
      }
      for(Sha256Hash h : hash_list)
      {
        md.update(h.getBytes());
        if (DEBUG)
        {
          debug_out.print(h);
          debug_out.print(" ");
        }

      }
      if (DEBUG) debug_out.print(") - ");

      byte[] pass = md.digest();
      md = MessageDigest.getInstance("SHA-256");
      md.update(pass);

      Sha256Hash out = new Sha256Hash(md.digest());
      if (DEBUG) debug_out.println(out);

      return out;

    }
    catch(java.security.NoSuchAlgorithmException e)
    {  
      throw new RuntimeException(e);
    }
    catch(org.apache.commons.codec.DecoderException e)
    {  
      throw new RuntimeException(e);
    }
  }

  public void notifyBlock(boolean wait_for_it)
  {
    synchronized(block_notify)
    {
      block_notify.notifyAll();
    }

      if (wait_for_it)
      {
        try
        {
          synchronized(block_done_notify)
          {
            block_done_notify.wait(15000);
          }
        }
        catch(Throwable t){}
      }
    
  }

  public static Sha256Hash getHashFromKey(String key)
  {
    String hash = key.substring(40, 40+64);

    return new Sha256Hash(hash);
  }

  public static String getKey(byte[] publicKey, Sha256Hash tx_id, int idx)
  {
    String addr_part = Hex.encodeHexString(publicKey);
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
    bb.putInt(idx);
    String idx_str = Hex.encodeHexString(bb.array());
    String key = addr_part + tx_id + idx_str;
    return key;

  }
    public String getKeyForInput(TransactionInput in)
    {
      if (in.isCoinBase()) return null;
      try
      {   
        byte[] public_key=null; 
        Address a = in.getFromAddress();
        public_key = a.getHash160();

        return getKey(public_key, in.getOutpoint().getHash(), (int)in.getOutpoint().getIndex());
      }
      catch(ScriptException e)
      {
        //Lets try this the other way
        try
        {   

          TransactionOutPoint out_p = in.getOutpoint();

          Transaction src_tx = tx_util.getTransaction(out_p.getHash());
          TransactionOutput out = src_tx.getOutput((int)out_p.getIndex());
          return getKeyForOutput(out, (int)out_p.getIndex());
        }
        catch(ScriptException e2)
        {   
          return null;
        }
      }

 
    }

    public static String getPrefixForAddress(Address a)
    {
      byte[] public_key=a.getHash160();
      return Hex.encodeHexString(public_key);
    }

    public static byte[] getPublicKeyForTxOut(TransactionOutput out, NetworkParameters params)
    {
        byte[] public_key=null;
        Script script = null;
            try
            {  
                script = out.getScriptPubKey();
                if (script.isSentToRawPubKey())
                {  
                    byte[] key = out.getScriptPubKey().getPubKey();
                    byte[] address_bytes = com.google.bitcoin.core.Utils.sha256hash160(key);

                    public_key = address_bytes;
                }
                else
                {  
                    Address a = script.getToAddress(params);
                    public_key = a.getHash160();
                }
            }
            catch(ScriptException e)
            { 
              if (script == null) return null;

              //com.google.bitcoin.core.Utils.sha256hash160 
              List<ScriptChunk> chunks = script.getChunks();
              /*System.out.println("STRANGE: " + out.getParentTransaction().getHash() + " - has strange chunks " + chunks.size());
              for(int i =0; i<chunks.size(); i++)
              {
                System.out.print("Chunk " + i + " ");
                System.out.print(Hex.encodeHex(chunks.get(i).data)); 
                System.out.println(" " + getOpCodeName(chunks.get(i).data[0]));
              }*/

              //Remember, java bytes are signed because hate
              if ((chunks.size() == 6) && (chunks.get(2).data.length == 20))
              if ((chunks.get(0).data.length == 1) && (chunks.get(0).data[0] == OP_DUP))
              if ((chunks.get(1).data.length == 1) && ((int)(chunks.get(1).data[0] & 0xFF) == OP_HASH160))
              if ((chunks.get(3).data.length == 1) && ((int)(chunks.get(3).data[0] & 0xFF) == OP_EQUALVERIFY))
              if ((chunks.get(4).data.length == 1) && ((int)(chunks.get(4).data[0] & 0xFF) == OP_CHECKSIG))
              if ((chunks.get(5).data.length == 1) && (chunks.get(5).data[0] == OP_NOP))
              {


                public_key = chunks.get(2).data;
              }
            }

      return public_key; 
    }

    public String getKeyForOutput(TransactionOutput out, int idx)
    {
      byte[] public_key = getPublicKeyForTxOut(out, params);
      if (public_key == null) return null;
      return getKey(public_key, out.getParentTransaction().getHash(), idx);
    }

  public class UtxoMgrThread extends Thread
  {
    private BlockChainCache block_chain_cache;

    public UtxoMgrThread()
    {
      setName("UtxoMgrThread");
      setDaemon(true);

      block_chain_cache = jelly.getBlockChainCache();

    }

    public void run()
    {
      recover();

      while(true)
      {
        while(catchup()){}
        waitForBlocks();
      }
      
      


    }
    private void recover()
    {
      UtxoStatus status = getUtxoState();

      if (!status.isConsistent())
      {
        Sha256Hash start = status.getPrevBlockHash();
        Sha256Hash end = status.getBlockHash();
        
        last_flush_block_hash = start;

        jelly.getEventLog().alarm("UTXO inconsistent, attempting recovery from " + start + " to " + end);
        LinkedList<Sha256Hash> recover_block_list = new LinkedList<Sha256Hash>();
        Sha256Hash ptr = end;
        while(!ptr.equals(start))
        {
          recover_block_list.addFirst(ptr);

          //System.out.println("Getting prev of : " + ptr);

          ptr = jelly.getDB().getBlockStoreMap().get(ptr).getHeader().getPrevBlockHash();
        }
        recover_block_list.addFirst(start);

        jelly.getEventLog().alarm("UTXO attempting recovery of " + recover_block_list.size() + " blocks");

        for(Sha256Hash blk_hash : recover_block_list)
        {
          Block b = jelly.getDB().getBlockMap().get(blk_hash).getBlock(jelly.getNetworkParameters());
          addBlock(b);
          
        }
        flush();

      }
      else
      {
        last_flush_block_hash = status.getBlockHash();
        last_added_block_hash = status.getBlockHash();
      }


    }
    int added_since_flush = 0;
    long last_flush = 0;
    private boolean catchup()
    {
      while(!block_chain_cache.isBlockInMainChain(last_added_block_hash))
      {
        rollback();
      }

      int curr_height = jelly.getDB().getBlockStoreMap().get(last_added_block_hash).getHeight();
      int head_height = jelly.getDB().getBlockStoreMap().get(
        jelly.getBlockChainCache().getHead()).getHeight();

      boolean near_caught_up=caught_up;

      for(int i=curr_height+1; i<=head_height; i++)
      {
        while(jelly.getSpaceLimited())
        {
          try
          {
            Thread.sleep(5000);
          }
          catch(Throwable t){}
        }
       
        Sha256Hash block_hash = jelly.getBlockChainCache().getBlockHashAtHeight(i);
        long t1=System.currentTimeMillis();
        SerializedBlock sb = jelly.getDB().getBlockMap().get(block_hash);
        if (sb == null) 
        {
          try{Thread.sleep(250); return true;}catch(Throwable t){}
        }
        caught_up=false;
        Block b = sb.getBlock(params);
        long t2=System.currentTimeMillis();

        get_block_stat.addDataPoint(t2-t1);

        if (b.getPrevBlockHash().equals(last_added_block_hash))
        {
          t1=System.currentTimeMillis();
          addBlock(b);
          t2=System.currentTimeMillis();

          add_block_stat.addDataPoint(t2-t1);
          Sha256Hash root_hash = getRootHash();


          block_height=i;

          checkUtxoHash(i, block_hash, root_hash);

          if ((near_caught_up) && (jelly.isUpToDate()))
          {
            jelly.getEventLog().alarm("UTXO added block " + i + " - " + root_hash);
          }
          else
          {
            jelly.getEventLog().log("UTXO added block " + i + " - " + root_hash);

          }


          added_since_flush++;

        }
        else
        {
          return true;
        }
        int flush_mod = 1000;
        //After the blocks get bigger, flush more often
        if (block_height > 220000) flush_mod = 100;
        
        if (i % flush_mod == 0)
        {
          flush();
          added_since_flush=0;
          last_flush = System.currentTimeMillis();
        }
      
      }

      synchronized(block_done_notify)
      {
        block_done_notify.notifyAll();
      }

      if ((added_since_flush > 0) && (last_flush +15000L < System.currentTimeMillis()))
      {
        flush();
        added_since_flush=0;
        last_flush = System.currentTimeMillis();
        
      }

      return false;


      
      

    }

    private void rollback()
    {
      jelly.getEventLog().alarm("UTXO rolling back " + last_added_block_hash);

      Sha256Hash prev = jelly.getDB().getBlockStoreMap().get(last_added_block_hash).getHeader().getPrevBlockHash();

      last_flush_block_hash = prev;
      Block b = jelly.getDB().getBlockMap().get(last_added_block_hash).getBlock(jelly.getNetworkParameters());
      rollbackBlock(b);

      //Setting hashes such that it looks like we are doing prev -> last_added_block_hash
      //That way on recovery we will re-add the block and then roll back again
      flush();

      last_added_block_hash = prev;


    }
    private void waitForBlocks()
    {
      caught_up=true;


      synchronized(block_done_notify)
      {
        block_done_notify.notifyAll();
      }
      synchronized(block_notify)
      {
        try
        {
          block_notify.wait(10000);
        }
        catch(InterruptedException e){}
      }


    }


  }

  private LinkedBlockingQueue<UtxoCheckEntry> check_queue = new LinkedBlockingQueue<UtxoCheckEntry>(8);

  public class UtxoCheckEntry
  {
    int height;
    Sha256Hash block;
    Sha256Hash utxo_root;
    public UtxoCheckEntry(int height, Sha256Hash block, Sha256Hash utxo_root)
    {
      this.height = height;
      this.block = block;
      this.utxo_root = utxo_root;
    }
  }

  public class UtxoCheckThread extends Thread
  {
    String client_name;
    public UtxoCheckThread()
    {
      setName("UtxoCheckThread");
      setDaemon(true);
      client_name = "j_" + StratumConnection.JELECTRUM_VERSION + "_";
      if (jelly.getConfig().isSet("irc_advertise_host"))
      {
        client_name += jelly.getConfig().get("irc_advertise_host");
      }
      else
      {
        client_name += new java.util.Random().nextInt();
      }
      //client_name = "check_file";

    }
    public void run()
    {
      while(true)
      {
        try
        {
          UtxoCheckEntry e = check_queue.take();
          String url = "https://jelectrum-1022.appspot.com/utxo?" 
            + "block=" + e.block.toString()
            + "&utxo=" + e.utxo_root.toString()
            + "&client="+ client_name;
          URL u = new URL(url);
          Scanner scan =new Scanner(u.openStream());
          String line = scan.nextLine();
          scan.close();



          StringTokenizer stok = new StringTokenizer(line, ",");
          String root_str = stok.nextToken();
          if (!root_str.equals("undetermined"))
          {
            Sha256Hash concur_root = new Sha256Hash(root_str);
            int matching = Integer.parseInt(stok.nextToken());
            int total = Integer.parseInt(stok.nextToken());
            if (!concur_root.equals(e.utxo_root))
            {
              jelly.getEventLog().alarm("UTXO check mismatch at " + e.height + " - me:" + e.utxo_root + " others:" + concur_root + " agreement " + + matching + " of " + total);  
              
            }
            else
            {
              jelly.getEventLog().log("UTXO check at " + e.height + " - " + concur_root + " - matching " + matching + " of " + total);
            }
          }
          else
          {
              jelly.getEventLog().log("UTXO check at " + e.height + " - " + root_str);
          }

        }
        catch(Throwable t)
        {
          jelly.getEventLog().alarm("UtxoCheckThread: " + t);
          t.printStackTrace();
        }

      }


    }

  }


  public static void main(String args[]) throws Exception
  {
    String config_path = args[0];
    Jelectrum jelly = new Jelectrum(new Config(config_path));


    int block_number = Integer.parseInt(args[1]);
    
    Sha256Hash block_hash = jelly.getBlockChainCache().getBlockHashAtHeight(block_number);
    Block b = jelly.getDB().getBlockMap().get(block_hash).getBlock(jelly.getNetworkParameters());
    System.out.println("Inspecting " + block_number + " - " + block_hash);



    int tx_count =0;
    int out_count =0;
    for(Transaction tx : b.getTransactions())
    {
      int idx=0;
      for(TransactionOutput tx_out : tx.getOutputs())
      {
        byte[] pub_key_bytes=getPublicKeyForTxOut(tx_out, jelly.getNetworkParameters());

        String public_key = null;
        if (pub_key_bytes != null) public_key = Hex.encodeHexString(pub_key_bytes);
        else public_key = "None";
        
        String script_bytes = Hex.encodeHexString(tx_out.getScriptBytes());

        String[] cmd=new String[3];
        cmd[0]="python";
        cmd[1]=jelly.getConfig().get("utxo_check_tool");
        cmd[2]=script_bytes;

        //System.out.println(script_bytes);

        Process p = Runtime.getRuntime().exec(cmd);

        Scanner scan = new Scanner(p.getInputStream());
        String ele_key = scan.nextLine();

        if (!ele_key.equals(public_key))
        {
          System.out.println("Mismatch on " + tx_out.getParentTransaction().getHash() + ":" + idx);
          System.out.println("  Script: " + script_bytes);
          System.out.println("  Jelectrum: " + public_key);
          System.out.println("  Electrum:  " + ele_key);

        }





        out_count++;
        idx++;
      }
      tx_count++;
    }
    System.out.println("TX Count: " + tx_count);
    System.out.println("Out Count: " + out_count);


  }

}
