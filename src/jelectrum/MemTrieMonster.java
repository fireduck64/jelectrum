
package jelectrum;

import java.util.Collection;
import java.util.LinkedList;
import java.security.MessageDigest;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Set;

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

import java.io.FileInputStream;
import java.util.Scanner;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Hex;
import java.sql.*;
import java.text.DecimalFormat;

/**
 * UTXO hash simulator to validate understanding.
 * It does not deal with re-orgs, partial blocks, databases
 * and the efficency is not great.
 * Blocks have to be loaded in order
 */
public class MemTrieMonster implements java.io.Serializable
{
  public static final boolean DEBUG=false;
  private transient NetworkParameters params;

  // A key is 20 bytes of public key, 32 bytes of transaction id and 4 bytes of output index
  int addr_space = 56;
 
  
  // Maps a 56 byte key (in hex) to a transaction id that is unspent for that key
  // Really the key already has the transaction id, so we could just have a set or a boolean
  // here
  TreeMap<String, Sha256Hash> addr_map = new TreeMap<String, Sha256Hash>();

  // Maps a partial key prefix to a tree node
  // The tree node has children, which are other prefixes or full keys
  TreeMap<String, SummaryNode> node_map = new TreeMap<String, SummaryNode>();

  // Maps transaction outpoints to the key that tracks it
  // This can be derived, but would involve reading the source transaction to
  // get its outputs to see what this one is spending
  HashMap<TransactionOutPoint, String> out_key_map = new HashMap<TransactionOutPoint, String>(8192, 0.5f);


  public MemTrieMonster(NetworkParameters params)
  {
    this.params = params;
    node_map.put("", new SummaryNode(""));
  }

  public void showSize() throws Exception
  {
    {
      System.out.println("addr_map: " + addr_map.size());
      System.out.println("node_map: " + node_map.size());
      System.out.println("out_key_map: " + out_key_map.size());

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      GZIPOutputStream zout = new GZIPOutputStream(bout);
      ObjectOutputStream oout=new ObjectOutputStream(zout);

      oout.writeObject(this);
      oout.flush();
      zout.finish();
      oout.close();

      double mb = bout.size() / 1048576.0;
      DecimalFormat df = new DecimalFormat("0.0");
      System.out.println("Serialized: " + df.format(mb) + " mb");
    }
    System.gc();
  }

  public synchronized void addBlock(Block b)
  {
    for(Transaction tx : b.getTransactions())
    {
      addTransaction(tx);
    }
  }
  public synchronized void addTransaction(Transaction tx)
  {
    
    int idx=0;
    for(TransactionOutput tx_out : tx.getOutputs())
    {
      String key = getKeyForOutput(tx_out, idx);
      if (DEBUG) System.out.println("Adding key: " + key);
      if (key != null)
      {
        node_map.get("").addHash(key, tx.getHash());

        TransactionOutPoint op = new TransactionOutPoint(params, idx, tx.getHash());
        out_key_map.put(op, key);
      }
      idx++;
    }

    for(TransactionInput tx_in : tx.getInputs())
    {
      if (!tx_in.isCoinBase())
      {
        markSpent(tx_in.getOutpoint());
      }
    }
  }


  private synchronized void markSpent(TransactionOutPoint out)
  {
      String key = out_key_map.get(out);

      node_map.get("").removeHash(key, out.getHash());

      addr_map.remove(key);
      out_key_map.remove(out);

  }

  public Sha256Hash getRootHash()
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

    public void setDirty()
    {
      dirty=true;
    }
    public void setClean()
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
          if (name.length() < addr_space*2)
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
      addr_map.put(key, tx_hash); 

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
        node_map.remove(prefix);
        return prefix + springs.first();
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
        if (full_sub.length() == addr_space*2)
        {
          h = addr_map.get(full_sub); 
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
    public String getKeyForOutput(TransactionOutput out, int idx)
    {
      String addr_part = null;
            try
            {  
                Script script = out.getScriptPubKey();
                if (script.isSentToRawPubKey())
                {  
                    byte[] key = out.getScriptPubKey().getPubKey();
                    byte[] address_bytes = com.google.bitcoin.core.Utils.sha256hash160(key);

                    addr_part = Hex.encodeHexString(address_bytes);
                    /*Address a = new Address(params, address_bytes);
                    addr_path = Hex.encodeHexString(a.getHash160());*/

                }
                else
                {  
                    Address a = script.getToAddress(params);
                    addr_part = Hex.encodeHexString(a.getHash160());
                }
            }
            catch(ScriptException e)
            {  

                //System.out.println(out.getParentTransaction().getHash() + " - " + out);
                //e.printStackTrace();
                //jelly.getEventLog().log("Unable process tx output: " + out.getParentTransaction().getHash());
              return null;
            }
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.order(java.nio.ByteOrder.LITTLE_ENDIAN);
      bb.putInt(idx);
      String idx_str = Hex.encodeHexString(bb.array());
      String key = addr_part + out.getParentTransaction().getHash() + idx_str;
      //return addr_part;
      return key;
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
      map.put(height, new Sha256Hash(hash));
    }
    return map;

  }


  public static void main(String args[]) throws Exception
  {

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
    

    Map<Integer, Sha256Hash> authMap = loadAuthMap("check/utxo-root-file");


    MemTrieMonster m = new MemTrieMonster(j.getNetworkParameters());

    int error=0;

    for(int i=1; i<200000; i++)
    {
      Sha256Hash block_hash = j.getBlockChainCache().getBlockHashAtHeight(i);
      m.addBlock(j.getDB().getBlockMap().get(block_hash).getBlock(j.getNetworkParameters()));

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
      if (i % 1000 == 0) m.showSize();

    }

    m.showSize();

    System.exit(0);


  }


}
