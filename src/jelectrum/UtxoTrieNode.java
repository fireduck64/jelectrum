
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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.commons.codec.binary.Hex;
import java.text.DecimalFormat;
import java.util.Random;
import lobstack.SerialUtil;

  public class UtxoTrieNode implements java.io.Serializable
  {
    private String prefix;

    private TreeMap<String, Sha256Hash> springs;


    public UtxoTrieNode(String prefix)
    {
      springs = new TreeMap<String, Sha256Hash>();
      this.prefix = prefix;
    }

    public UtxoTrieNode(ByteBuffer bb)
      throws java.io.IOException
    {
      DataInputStream din=new DataInputStream(new ByteArrayInputStream(bb.array()));

      prefix = SerialUtil.readString(din);
      int count = din.readInt();

      byte hash_bytes[]=new byte[32];
      springs = new TreeMap<String, Sha256Hash>();

      for(int i=0; i<count; i++)
      {
        String sub = SerialUtil.readString(din);
        din.readFully(hash_bytes);
        Sha256Hash hash = new Sha256Hash(hash_bytes);
        if (hash.toString().equals("0000000000000000000000000000000000000000000000000000000000000000")) hash=null;
        springs.put(sub, hash);

      }
      
    }

    public ByteBuffer serialize()
      throws java.io.IOException
    {
      ByteArrayOutputStream b_out = new ByteArrayOutputStream();
      DataOutputStream d_out = new DataOutputStream(b_out);

      SerialUtil.writeString(d_out, prefix);
      d_out.writeInt(springs.size());
      for(Map.Entry<String, Sha256Hash> me : springs.entrySet())
      {
        SerialUtil.writeString(d_out, me.getKey());
        Sha256Hash hash = me.getValue();

        if (hash == null) hash = new Sha256Hash("0000000000000000000000000000000000000000000000000000000000000000");
        d_out.write(hash.getBytes());
      }

      d_out.flush();


      return ByteBuffer.wrap(b_out.toByteArray());
    }



    public void addSpring(String s, UtxoTrieMgr mgr)
    {
      springs.put(s, null);
      mgr.putSaveSet(prefix, this);
    }
    public void addHash(String key, Sha256Hash tx_hash, UtxoTrieMgr mgr)
    {
      mgr.putSaveSet(prefix, this);

      String next = key.substring(prefix.length());
      for(String sub : springs.keySet())
      {
        if (next.startsWith(sub))
        {
          String name = prefix+sub;
          if (name.length() < UtxoTrieMgr.ADDR_SPACE*2)
          { //Handles strange txid d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599 issue
            //If the sub name is the entire key space, then we are adding a duplicate transaction and
            //are just going to leave that alone
            UtxoTrieNode n = mgr.getByKey(prefix+sub);
            if (n == null) System.out.println("Missing: " + prefix + sub + " from " + prefix);
            n.addHash(key, tx_hash, mgr);
            springs.put(sub, null);
          }
          return;
        }
      }
      for(String sub : springs.keySet())
      {
        int common = UtxoTrieMgr.commonLength(sub, next);
        if (common >= 2)
        {
          String common_str = sub.substring(0, common);

          springs.remove(sub);
          springs.put(common_str, null);

          UtxoTrieNode n = new UtxoTrieNode(prefix + common_str);

          n.addHash(key, tx_hash, mgr);
          n.addSpring(sub.substring(common), mgr);
           
          return;
        }
        
      }
      springs.put(next, null);

    }

    public String removeHash(String key, Sha256Hash tx_hash, UtxoTrieMgr mgr)
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
            String next_sub = mgr.getByKey(prefix+sub).removeHash(key, tx_hash, mgr);
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
      /*if ((!dirty) && (skip_string.equals(last_skip)))
      {
        return hash;
      }
      last_skip = skip_string;*/

      LinkedList<Sha256Hash> lst=new LinkedList<Sha256Hash>();

      for(String sub : springs.keySet())
      {
        if (springs.get(sub) != null)
        {
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
          {
            h = UtxoTrieMgr.getHashFromKey(full_sub);
          }
          else
          {
            h = mgr.getByKey(full_sub).getHash(sub_skip_str, mgr);
          }
          lst.add(h);

          springs.put(sub, h);
        }
      }

      Sha256Hash hash = null;

      
      if ((lst.size() == 1) && (prefix.length() >= 2))
      {
        hash = lst.get(0);
      }
      else
      {
        hash = UtxoTrieMgr.hashThings(skip_string,lst);
      }

      return hash;
    }
  }


