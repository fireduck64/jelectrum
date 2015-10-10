
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

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ScriptException;
import org.bitcoinj.core.Address;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;

import java.io.FileInputStream;
import java.util.Scanner;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PrintStream;
import com.google.protobuf.ByteString;

import org.apache.commons.codec.binary.Hex;
import java.text.DecimalFormat;
import java.util.Random;
import lobstack.SerialUtil;
import org.junit.Assert;
import java.io.OutputStream;

 
/**
 * This class stores a node in the UTXO trie structure
 */
public class UtxoTrieNode implements java.io.Serializable
{

  /**
   * This is the key prefix of this node.
   * It is in hex, 20 bytes (40 characters) of address public key
   * then 32 bytes (64 characters) of transaction hash
   * then 4 bytes of transaction output offset (integer)
   * but none of those code cares about that.  It is just a string
   * that could be up to 56*2 characters long
   */
  private String prefix;

  /**
   * This is a map children of this node.
   * For space efficency, the string keys in this map
   * are only the part after this prefix.
   * So if this node is "12" and the child prefix is "12af" then this map will just have "af".
   *
   * The hash value is the hash of the subtree or null if it needs to be recalculated.
   * this way, when getHash() is called we know which children we need to recurse into
   */
  private TreeMap<String, Sha256Hash> springs;

  /**
   * For some serialization methods we want a special null value
   * so this value, which is a hash of "null" is our chosen null value.
   */
  private static Sha256Hash hash_null = new Sha256Hash("74234e98afe7498fb5daf1f36ac2d78acc339464f950703b8c019892f982b90b");
  public static final long serialVersionUID = 2675325841660230241L;



  public UtxoTrieNode(String prefix)
  {
    springs = new TreeMap<String, Sha256Hash>();
    this.prefix = prefix;
  }

  /**
   * Deserialize from a byte string
   */
  public UtxoTrieNode(ByteString bs)
  {
    try
    {
      DataInputStream din=new DataInputStream(bs.newInput());

      long ver = din.readLong();
      Assert.assertEquals(serialVersionUID, ver);

      prefix = SerialUtil.readString(din);
      int count = din.readInt();

      springs = new TreeMap<String, Sha256Hash>();

      for(int i=0; i<count; i++)
      {
        byte hash_bytes[]=new byte[32];
        String sub = SerialUtil.readString(din);
        din.readFully(hash_bytes);
        Sha256Hash hash = new Sha256Hash(hash_bytes);
        
        if (hash.equals(hash_null))
        {
          hash=null;
          springs.put(sub, null);
        }
        else
        {
          springs.put(sub, hash);
        }

      }
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
    
  }

  public String getPrefix()
  {
    return prefix;
  }
  public Map<String, Sha256Hash> getSprings()
  {
    return springs;
  }

  public void dumpDB(OutputStream out, UtxoTrieMgr mgr)
    throws java.io.IOException
  {
    ByteString self = serialize();
    byte[] sz = new byte[4];
    ByteBuffer bb = ByteBuffer.wrap(sz);
    bb.putInt(self.size());

    out.write(sz);
    out.write(self.toByteArray());
    for(Map.Entry<String, Sha256Hash> me : springs.entrySet())
    {
      String sub = prefix + me.getKey();
      
      if (sub.length() < UtxoTrieMgr.ADDR_SPACE*2)
      {
        mgr.getByKey(sub).dumpDB(out, mgr);
      }

    }

  }

  /**
   * Serialize to a byte string
   */
  public ByteString serialize()
  {
    try
    {
      ByteArrayOutputStream b_out = new ByteArrayOutputStream();
      DataOutputStream d_out = new DataOutputStream(b_out);

      d_out.writeLong(serialVersionUID);

      SerialUtil.writeString(d_out, prefix);
      d_out.writeInt(springs.size());
      for(Map.Entry<String, Sha256Hash> me : springs.entrySet())
      {

        SerialUtil.writeString(d_out, me.getKey());
        Sha256Hash hash = me.getValue();

        if (hash == null) hash = hash_null;
        
        d_out.write(hash.getBytes());
      }

      d_out.flush();


      return ByteString.copyFrom(b_out.toByteArray());
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
  }



  /**
   * This should be called add child and mark as needing to be rehashed
   */
  public void addSpring(String s, UtxoTrieMgr mgr)
  {
    // Mark that we don't have the hash
    springs.put(s, null);

    // Mark this node as changes to it needs to be saved on next flush to db
    mgr.putSaveSet(prefix, this);
  }

  /**
   * Return an ordered set of keys matching the given 'start' prefix
   */
  public Collection<String> getKeysByPrefix(String start, UtxoTrieMgr mgr)
  {
    LinkedList<String> lst = new LinkedList<>();
    for(String sub : springs.keySet())
    {
      String name = prefix+sub;
      if (name.startsWith(start) || start.startsWith(name))
      {
        //If it is the expected total length, then it is just a leaf node
        //and we can just put it on the list
        if (name.length() == UtxoTrieMgr.ADDR_SPACE*2)
        {
          lst.add(name);
        }
        else
        {
          UtxoTrieNode n = mgr.getByKey(name);
          if (n == null) System.out.println("Missing: " + name + " from " + prefix);
          lst.addAll(n.getKeysByPrefix(start, mgr));
        }
      }
    }
    return lst;

  }

  public void cacheNodes(String key, UtxoTrieMgr mgr)
  {
    mgr.putSaveSet(prefix, this);

    for(String sub : springs.keySet())
    {
      String name = prefix+sub;
      if (key.startsWith(name))
      {
        if (name.length() < UtxoTrieMgr.ADDR_SPACE*2)
        {
          UtxoTrieNode n = mgr.getByKey(name);
          n.cacheNodes(key, mgr);
        }
      }
    }
  }


  public void addHash(String key, UtxoTrieMgr mgr)
  {
    mgr.putSaveSet(prefix, this);

    //If we are here, we are assuming that the start of 'key' and
    //my 'prefix' are already matching and that 'key' is longer.
    //So get just the part of 'key' that is past 'prefix'.
    //Example:
    // If this node is "abc7" and we are adding "abc7f8f8fe"
    // Then next will be "f8f8fe"
    Assert.assertTrue(key.startsWith(prefix));
    String next = key.substring(prefix.length());


    for(String sub : springs.keySet())
    {
      //If the new key simply fits into a sub node we have already, send it there
      if (next.startsWith(sub))
      {
        String name = prefix+sub;
        if (name.length() < UtxoTrieMgr.ADDR_SPACE*2)
        { //if statement avoids the strange txid d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599 issue
          //If the sub name is the entire key space, then we are adding a duplicate transaction and
          //are just going to leave that alone

          //Otherwise, add it to the node below us
          UtxoTrieNode n = mgr.getByKey(prefix+sub);
          if (n == null) System.out.println("Missing: " + prefix + sub + " from " + prefix);
          n.addHash(key, mgr);
          springs.put(sub, null);
        }
        return;
      }
    }

    for(String sub : springs.keySet())
    {
      int common = UtxoTrieMgr.commonLength(sub, next);

      //If the new entry has a common start with a previous entry
      //Make a new sub node that will contain them both
      if (common >= 2)
      {
        String common_str = sub.substring(0, common);

        springs.remove(sub);
        springs.put(common_str, null);

        UtxoTrieNode n = new UtxoTrieNode(prefix + common_str);

        n.addHash(key, mgr);
        n.addSpring(sub.substring(common), mgr);
         
        return;
      }
      
    }

    //If it doesn't go into a sub node
    //and it has no common node, just save it directly to this node
    springs.put(next, null);

  }

  public String removeHash(String key, UtxoTrieMgr mgr)
  {
    mgr.putSaveSet(prefix, this);
    String rest = key.substring(prefix.length());
    if (springs.containsKey(rest))
    {
      springs.remove(rest);
    }
    else
    {
      for(String sub : springs.keySet())
      {
        String full = prefix + sub;
        if (rest.startsWith(sub))
        {
          String next_sub = mgr.getByKey(prefix+sub).removeHash(key, mgr);
          springs.put(sub, null);

          if (next_sub == null)
          {
            springs.remove(sub);
          }
          if (next_sub != full)
          {
            springs.remove(sub);
            springs.put(next_sub.substring(prefix.length()),null);
          }
          break;
        }
      }

    }
    if (springs.size() == 0)
    {
      //node_map.remove(prefix);
      return null;
    }
    if (springs.size() == 1)
    {
      String ret = prefix + springs.firstKey();
      springs.put(springs.firstKey(), null);

      //node_map.remove(prefix);
      //springs.clear();
      //We are not clearing springs in case we get a partial save
      //and this node is still referenced
      //in which case, we don't want to lose track of the nodes under this one
      return ret;
    }
    return prefix;


  }


  public Sha256Hash getHash(String skip_string, UtxoTrieMgr mgr)
  {

    LinkedList<Sha256Hash> lst=new LinkedList<Sha256Hash>();

    boolean changed=false;

    for(String sub : springs.keySet())
    {
      /*if (hash_null.equals(springs.get(sub)))
      {
        springs.put(sub, null);
      }*/
      if (springs.get(sub) != null)
      { //If we have a hash for a child already, just use it
        lst.add(springs.get(sub));
      }
      else
      {

        String sub_skip_str = "";
        if (sub.length() > 2)
        {
          sub_skip_str = sub.substring(2); 
        }
        String full_sub = prefix+sub;
        Sha256Hash h = null;
        if (full_sub.length() == UtxoTrieMgr.ADDR_SPACE*2)
        { // If the sub is a leaf, just get the tx hash
          h = UtxoTrieMgr.getHashFromKey(full_sub);
        }
        else
        { // Otherwise, recurse
          h = mgr.getByKey(full_sub).getHash(sub_skip_str, mgr);
        }
        lst.add(h);

        // Save any hash we calculate for the child for later use
        springs.put(sub, h);
        changed=true;
      }
    }
    if (changed)
    {
      mgr.putSaveSet(prefix, this);
    }

    Sha256Hash hash = null;

    
    if ((lst.size() == 1) && (prefix.length() >= 2))
    { // I don't want to talk about it
      hash = lst.get(0);
    }
    else
    { //Take the skip list and the sub hashes and hash them
      hash = UtxoTrieMgr.hashThings(skip_string,lst);
    }

    return hash;
  }


  public void printTree(PrintStream out, int indent, UtxoTrieMgr mgr)
  {
    for(int i=0; i<indent; i++) out.print(" ");
    out.println("Node: ." + prefix +".");
    indent++;
    for(Map.Entry<String, Sha256Hash> me : springs.entrySet())
    {
      String sub = prefix + me.getKey();
      
      for(int i=0; i<indent; i++) out.print(" ");
      out.println(sub + " - " + me.getValue());
      if (sub.length() < UtxoTrieMgr.ADDR_SPACE*2)
      {
        mgr.getByKey(sub).printTree(out, indent+1, mgr);
      }

    }

  }
}

