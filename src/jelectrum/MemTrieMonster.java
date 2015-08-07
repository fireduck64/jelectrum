
package jelectrum;

import java.util.Collection;
import java.util.LinkedList;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Set;
import java.util.List;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.nio.ByteBuffer;

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
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Hex;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Random;

/**
 * UTXO hash simulator to validate understanding.
 * Blocks have to be loaded in order
 */
public class MemTrieMonster implements java.io.Serializable
{
  // A key is 20 bytes of public key, 32 bytes of transaction id and 4 bytes of output index
  public static final int ADDR_SPACE = 56;
  public static boolean DEBUG=false;

  private transient NetworkParameters params;
  private transient Jelectrum jelly;

  // Maps a partial key prefix to a tree node
  // The tree node has children, which are other prefixes or full keys
  protected TreeMap<String, SummaryNode> node_map = new TreeMap<String, SummaryNode>();


  public MemTrieMonster(Jelectrum jelly, NetworkParameters params)
  {
    this.jelly = jelly;
    this.params = params;
    node_map.put("", new SummaryNode(""));
  }

  public void showSize() throws Exception
  {
    {
      System.out.println("node_map: " + node_map.size());

      /*ByteArrayOutputStream bout = new ByteArrayOutputStream();
      GZIPOutputStream zout = new GZIPOutputStream(bout);
      ObjectOutputStream oout=new ObjectOutputStream(zout);

      oout.writeObject(this);
      oout.flush();
      zout.finish();
      oout.close();

      double mb = bout.size() / 1048576.0;
      DecimalFormat df = new DecimalFormat("0.0");
      System.out.println("Serialized: " + df.format(mb) + " mb");*/
    }
  }

  public synchronized void addBlock(Block b)
  {
    for(Transaction tx : b.getTransactions())
    {
      addTransaction(tx);
    }
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



  

  private void addTransaction(Transaction tx)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      String key = getKeyForOutput(tx_out, idx);
      if (DEBUG) System.out.println("Adding key: " + key);
      if (key != null)
      {
        node_map.get("").addHash(key, tx.getHash());

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
          node_map.get("").removeHash(key, tx_in.getOutpoint().getHash());
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
        node_map.get("").removeHash(key, tx.getHash());

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
          node_map.get("").addHash(key, tx_in.getOutpoint().getHash());
        }
      }
    }
  }


  public synchronized Sha256Hash getRootHash()
  {
    if (DEBUG) System.out.println(node_map);
    return node_map.get("").getHash("");

  }


  public class SummaryNode implements java.io.Serializable
  {
    private String prefix;
    private Sha256Hash hash;
    private boolean dirty;
    private String last_skip=null;

    private TreeSet<String> springs;


    public SummaryNode(String prefix)
    {
      dirty=true;
      springs = new TreeSet<String>();
      this.prefix = prefix;
    }

    private void setDirty()
    {
      dirty=true;
    }
    private void setClean()
    {
      dirty=false;
    }

    public void setHash(Sha256Hash hash){ this.hash = hash; }

    public String toString()
    {
      //return "{" + springs + "}";
      return "{" +springs.size() + "}";
    }

    public void addSpring(String s)
    {
      springs.add(s);
      node_map.put(prefix, this);
    }
    public void addHash(String key, Sha256Hash tx_hash)
    {
      setDirty();

      String next = key.substring(prefix.length());
      for(String sub : springs)
      {
        if (next.startsWith(sub))
        {
          String name = prefix+sub;
          if (name.length() < ADDR_SPACE*2)
          { //Handles strange txid d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599 issue
            //If the sub name is the entire key space, then we are adding a duplicate transaction and
            //are just going to leave that alone
            SummaryNode n = node_map.get(prefix+sub);
            if (n == null) System.out.println("Missing: " + prefix + sub + " from " + prefix);
            n.addHash(key, tx_hash);
          }
          return;
        }
      }
      for(String sub : springs)
      {
        int common = commonLength(sub, next);
        if (common >= 2)
        {
          String common_str = sub.substring(0, common);

          springs.remove(sub);
          springs.add(common_str);

          SummaryNode n = new SummaryNode(prefix + common_str);

          node_map.put(prefix + common_str, n);
          n.addHash(key, tx_hash);
          n.addSpring(sub.substring(common));
           
          return;
        }
        
      }
      springs.add(next);

    }

    public String removeHash(String key, Sha256Hash tx_hash)
    {
      setDirty();
      String rest = key.substring(prefix.length());
      if (springs.contains(rest))
      {
        springs.remove(rest);
      }
      else
      {
        for(String sub : springs)
        {
          String full = prefix + sub;
          if (rest.startsWith(sub))
          {
            String next_sub = node_map.get(prefix+sub).removeHash(key, tx_hash);
            if (next_sub == null)
            {
              springs.remove(sub);
            }
            if (next_sub != full)
            {
              springs.remove(sub);
              springs.add(next_sub.substring(prefix.length()));
            }
            break;
          }
        }

      }
      if (springs.size() == 0)
      {
        node_map.remove(prefix);
        return null;
      }
      if (springs.size() == 1)
      {
        String ret = prefix + springs.first();
        node_map.remove(prefix);
        springs.clear();
        return ret;
      }
      return prefix;


    }

    public Sha256Hash getHash(String skip_string)
    {
      if ((!dirty) && (skip_string.equals(last_skip)))
      {
        return hash;
      }
      last_skip = skip_string;

      LinkedList<Sha256Hash> lst=new LinkedList<Sha256Hash>();

      for(String sub : springs)
      {
        String sub_skip_str = "";
        if (sub.length() > 2)
        {
          sub_skip_str = sub.substring(2); 
        }
        String full_sub = prefix+sub;
        Sha256Hash h = null;
        if (full_sub.length() == ADDR_SPACE*2)
        {
          h = getHashFromKey(full_sub);
        }
        else
        {
          h = node_map.get(full_sub).getHash(sub_skip_str);
        }
        lst.add(h);
      }

      
      if ((lst.size() == 1) && (prefix.length() >= 2))
      {
        setHash(lst.get(0));
        setClean();
      }
      else
      {
        setHash(hashThings(skip_string,lst));
        setClean();
      }

      return hash;
    }
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
      
      if (DEBUG) System.out.print("hash(");

      MessageDigest md = MessageDigest.getInstance("SHA-256");
      if ((skip != null) && (skip.length() > 0))
      {
        byte[] skipb = Hex.decodeHex(skip.toCharArray());
        md.update(skipb);
        if (DEBUG) 
        {
          System.out.print("skip:");
          System.out.print(skip);
          System.out.print(' ');
        }
      }
      for(Sha256Hash h : hash_list)
      {
        md.update(h.getBytes());
        if (DEBUG)
        {
          System.out.print(h);
          System.out.print(" ");
        }

      }
      if (DEBUG) System.out.print(") - ");

      byte[] pass = md.digest();
      md = MessageDigest.getInstance("SHA-256");
      md.update(pass);

      Sha256Hash out = new Sha256Hash(md.digest());
      if (DEBUG) System.out.println(out);

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

  public Sha256Hash getHashFromKey(String key)
  {
    String hash = key.substring(40, 40+64);

    return new Sha256Hash(hash);
  }

  public String getKey(byte[] publicKey, Sha256Hash tx_id, int idx)
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

          Transaction src_tx = jelly.getImporter().getTransaction(out_p.getHash());
          TransactionOutput out = src_tx.getOutput((int)out_p.getIndex());
          return getKeyForOutput(out, (int)out_p.getIndex());
        }
        catch(ScriptException e2)
        {   
          return null;
        }
      }

 
    }

    public String getKeyForOutput(TransactionOutput out, int idx)
    {
        byte[] public_key=null;
        Script script = out.getScriptPubKey();
            try
            {  
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
              //com.google.bitcoin.core.Utils.sha256hash160 
              List<ScriptChunk> chunks = script.getChunks();
              System.out.println("STRANGE: " + out.getParentTransaction().getHash() + ":" + idx + " - has strange chunks " + chunks.size());
              if ((chunks.size() == 6) && (chunks.get(2).data.length == 20))
              {
                public_key = chunks.get(2).data;
              }
              else
              {

                return null;
              }
            }

      return getKey(public_key, out.getParentTransaction().getHash(), idx);
    }


  public static Map<Integer, Sha256Hash> loadAuthMap(String location)
    throws Exception
  {
    Scanner scan =new Scanner(new FileInputStream(location));

    TreeMap<Integer, Sha256Hash> map = new TreeMap<Integer, Sha256Hash>();

    while(scan.hasNext())
    {
      int height = scan.nextInt();
      String hash = scan.next();
      try
      {
      map.put(height, new Sha256Hash(hash));
      }
      catch(Throwable t)
      {}
    }
    System.out.println("Loaded checks: " + map.size());
    return map;

  }


  public static void main(String args[]) throws Exception
  {

  StatData get_block_stat=new StatData();
  StatData add_block_stat=new StatData();
 
    Jelectrum j = new Jelectrum(new Config("jelly.conf"));

    /*Connection conn = DB.openConnection("jelectrum_db");
    for(int i=367800; i<368000; i++)
    {
      Sha256Hash block_hash = j.getBlockChainCache().getBlockHashAtHeight(i);
      if (block_hash != null)
      {
        System.out.println("" + i + " - " + block_hash);
        PreparedStatement ps = conn.prepareStatement("delete from block_map where key=?");
        ps.setString(1, block_hash.toString());
        ps.execute();
        ps.close();
      }
    }
    conn.close();*/

    {
      Transaction tx = j.getImporter().getTransaction(new Sha256Hash("24d5ee912ade13c7b5ab7813cc910b0372efee4d8a61345ed6a2c626084ca6f0"));
      TransactionOutput out = tx.getOutputs().get(0);

      Script script = out.getScriptPubKey();
      byte[] b=script.getProgram();

      System.out.println(Hex.encodeHexString(b));

      List<ScriptChunk> chunks = script.getChunks();
      for(ScriptChunk c : chunks)
      {
        System.out.println(Hex.encodeHexString(c.data));
      }

      //System.exit(-1);

    }
    

    Map<Integer, Sha256Hash> authMap = loadAuthMap("check/utxo-root-file");


    MemTrieMonster m = new MemTrieMonster(j, j.getNetworkParameters());

    int error=0;

    Random rnd = new Random();
    DecimalFormat df = new DecimalFormat("0.0");

    for(int i=1; i<368694; i++)
    {
      //if (i == 127630) DEBUG=true;

      Sha256Hash block_hash = j.getBlockChainCache().getBlockHashAtHeight(i);

      Sha256Hash prev_root_hash = m.getRootHash();

      long t1=System.currentTimeMillis();
      Block b = j.getDB().getBlockMap().get(block_hash).getBlock(j.getNetworkParameters());
      long t2=System.currentTimeMillis();

      get_block_stat.addDataPoint(t2-t1);

      t1=System.currentTimeMillis();
      m.addBlock(b);
      t2=System.currentTimeMillis();
      add_block_stat.addDataPoint(t2-t1);

      if (rnd.nextDouble() < 0.01)
      {
        //All changes should be eidempotent
        //which we will depend on if an update is interupted mid stream
        //We'll just continue to add the block being worked on
        System.out.println("Adding again for lolz");
        m.addBlock(b);
      }
      if (rnd.nextDouble() < 0.005)
      {
        //We should be able to roll back a block and get to the previous state
        //and then roll forward again
        System.out.println("Rolling back");
        m.rollbackBlock(b);
        Sha256Hash root_hash_now = m.getRootHash();
        System.out.println("Roll back: " + prev_root_hash + " - " + root_hash_now);
        if (!prev_root_hash.equals(root_hash_now)) error++;
        m.addBlock(b);
      }


      Sha256Hash root_hash = m.getRootHash();

      //System.out.println(m.node_map);

      if (authMap.containsKey(i))
      {

        System.out.println("Height: " + i + " - " + root_hash + " - " + authMap.get(i));
        if (!root_hash.equals(authMap.get(i))) error++;
      }
      else
      {
        System.out.println("Height: " + i + " - " + root_hash);

      }

      //if (error>5) return;
      if (error>0) System.exit(-1);
      if (i % 1000 == 0)
      {
        m.showSize();
        get_block_stat.print("get_block", df);
        add_block_stat.print("add_block", df);
        System.gc();
      }

    }

    m.showSize();


    System.exit(0);


  }


}
