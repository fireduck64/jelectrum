package jelectrum;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.Collection;

import lobstack.Lobstack;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import java.nio.ByteBuffer;
import com.google.bitcoin.core.Sha256Hash;

public class LobstackMap<K,V> implements Map<K, V>
{
  private Lobstack stack;

  private ConversionMode mode;

  public enum ConversionMode
  {
    STRING,
    SHA256HASH,
    OBJECT,
    SERIALIZEDTRANSACTION,
    UTXONODE
  }


    public LobstackMap(Lobstack stack, ConversionMode mode)
    {
      this.mode = mode;
      this.stack = stack;
    }

    public void clear()
    {
        throw new RuntimeException("not implemented - is stupid");
    }


    public boolean containsKey(Object key)
    {
      try
      {
        return (stack.get(key.toString()) != null);
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }
    public boolean containsValue(Object value)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public  boolean equals(Object o)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V get(Object key)
    {
      try
      {
        ByteBuffer buff = stack.get(key.toString());
        if (buff == null) return null;

        if (mode==ConversionMode.STRING)
        {
          return (V) new String(buff.array());
        }
        if (mode==ConversionMode.SHA256HASH)
        {
          return (V) new Sha256Hash(buff.array());
        }
        if (mode==ConversionMode.OBJECT)
        {
          try
          {
            ByteArrayInputStream bin = new ByteArrayInputStream(buff.array());
            ObjectInputStream oin = new ObjectInputStream(bin);

            return (V) oin.readObject();
          }
          catch(java.io.IOException e)
          {
            throw new RuntimeException(e);
          }
          catch(ClassNotFoundException e)
          {
            throw new RuntimeException(e);
          }

        }
        if (mode==ConversionMode.SERIALIZEDTRANSACTION)
        {
          return (V) new SerializedTransaction(buff.array());
        }
        if (mode==ConversionMode.UTXONODE)
        {
          return (V) new UtxoTrieNode(buff);
        }
        throw new RuntimeException("No conversion found");
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}

    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public boolean isEmpty()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public int size()
    {
        throw new RuntimeException("not implemented - is stupid");
    }


    public  Set<K>  keySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value) 
    {
      try
      {
        ByteBuffer b = convertV(value);
        stack.put(key.toString(), b);

        return null;
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }

    private ByteBuffer convertV(V value)
      throws java.io.IOException
    {
      ByteBuffer b = null;
      if (value != null)
      {
        if (mode==ConversionMode.STRING)
        {
          b = ByteBuffer.wrap(value.toString().getBytes());
        }
        if (mode==ConversionMode.SHA256HASH)
        {
          Sha256Hash h = (Sha256Hash) value;
          b = ByteBuffer.wrap(h.getBytes());
        }
        if (mode==ConversionMode.OBJECT)
        {
          try
          {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream oout = new ObjectOutputStream(bout);
            oout.writeObject(value);
            oout.flush();
            b = ByteBuffer.wrap(bout.toByteArray());
          }
          catch(java.io.IOException e)
          {
            throw new RuntimeException(e);
          }

        }
        if (mode==ConversionMode.SERIALIZEDTRANSACTION)
        {
          SerializedTransaction stx = (SerializedTransaction)value;
          b = ByteBuffer.wrap(stx.getBytes());
        }
        if (mode==ConversionMode.UTXONODE)
        {
          UtxoTrieNode node = (UtxoTrieNode) value;
          return node.serialize();
        }


      }
      return b;
 
    }

    public void putAll(Map<? extends K,? extends V> m) 
    {
      try
      {
        Map<String, ByteBuffer> write_map = new TreeMap<String, ByteBuffer>();

        for(Map.Entry<? extends K,? extends V> me : m.entrySet())
        {
          write_map.put(me.getKey().toString(), convertV(me.getValue()));
        }
        stack.putAll(write_map);
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}

      
    }

    public V remove(Object key) 
    {
      throw new RuntimeException("not implemented - is stupid");
    }

    public Collection<V>   values() 
    {
      throw new RuntimeException("not implemented - is stupid");
    }

}
