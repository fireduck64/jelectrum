package jelectrum.db;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Collection;
import java.nio.ByteBuffer;
import com.google.protobuf.ByteString;

import jelectrum.UtxoTrieNode;
import jelectrum.SerializedTransaction;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import com.google.bitcoin.core.Sha256Hash;

/**
 * Converts the high level types to and from the simple
 * ByteStrings used by the DB layer.
 * Knows how to efficently convert some of our well used types
 * and falls back to native java serialization.
 */
public class ObjectConversionMap<K, V> implements Map<K, V>
{
  private DBMap inner;

  private ConversionMode mode;

  public enum ConversionMode
  {
    STRING,
    SHA256HASH,
    OBJECT,
    SERIALIZEDTRANSACTION,
    UTXONODE
  } 


  public ObjectConversionMap(ConversionMode mode, DBMap inner)
  {
    this.inner = inner;
    this.mode = mode;

  }
  
  public boolean containsKey(Object key)
  {
    String k = key.toString();
    return inner.containsKey(k);
  }

  public V get(Object key)
  { 
    String k = key.toString();
    ByteString buff = inner.get(k);
    if (buff == null) return null;

    if (mode==ConversionMode.STRING)
    {
      return (V) buff.toStringUtf8();
    }
    if (mode==ConversionMode.SHA256HASH)
    {
      return (V) new Sha256Hash(buff.toByteArray());
    }
    if (mode==ConversionMode.OBJECT)
    {
      try
      {
        ObjectInputStream oin = new ObjectInputStream(buff.newInput());

        return (V) oin.readObject();
      }
      catch(java.io.IOException e)
      {
        System.out.println("Exception reading key: " + key);
        throw new RuntimeException(e);
      }
      catch(ClassNotFoundException e)
      {
        throw new RuntimeException(e);
      }

    }
    if (mode==ConversionMode.SERIALIZEDTRANSACTION)
    {
      return (V) new SerializedTransaction(buff.toByteArray());
    }
    if (mode==ConversionMode.UTXONODE)
    {
      return (V) new UtxoTrieNode(ByteBuffer.wrap(buff.toByteArray()));
    }
    throw new RuntimeException("No conversion found");

  }


  public V put(K key, V value)
  { 
    try
    { 
      ByteString b = convertV(value);
      inner.put(key.toString(), b);
    }
    catch(java.io.IOException e){throw new RuntimeException(e);}
    return null;
  }

  public void putAll(Map<? extends K,? extends V> m)
  { 
    try
    {
      Map<String, ByteString> write_map = new TreeMap<String, ByteString>();

      for(Map.Entry<? extends K,? extends V> me : m.entrySet())
      {
        write_map.put(me.getKey().toString(), convertV(me.getValue()));
      }
      inner.putAll(write_map);
    }
    catch(java.io.IOException e){throw new RuntimeException(e);}

  }

  private ByteString convertV(V value)
    throws java.io.IOException
  {
    ByteString b = null;
    if (value != null)
    {
      if (mode==ConversionMode.STRING)
      {
        b = ByteString.copyFromUtf8(value.toString());
      }
      if (mode==ConversionMode.SHA256HASH)
      {
        Sha256Hash h = (Sha256Hash) value;
        b = ByteString.copyFrom(h.getBytes());
      }
      if (mode==ConversionMode.OBJECT)
      {
        try
        {
          ByteArrayOutputStream bout = new ByteArrayOutputStream();
          ObjectOutputStream oout = new ObjectOutputStream(bout);
          oout.writeObject(value);
          oout.flush();
          b = ByteString.copyFrom(bout.toByteArray());
        }
        catch(java.io.IOException e)
        {
          throw new RuntimeException(e);
        }

      }
      if (mode==ConversionMode.SERIALIZEDTRANSACTION)
      {
        SerializedTransaction stx = (SerializedTransaction)value;
        b = ByteString.copyFrom(stx.getBytes());
      }
      if (mode==ConversionMode.UTXONODE)
      {
        UtxoTrieNode node = (UtxoTrieNode) value;
        b = ByteString.copyFrom(node.serialize().array());
      }


    }
    return b;

  }




  public void clear()
  {   
    throw new RuntimeException("not implemented - is stupid");
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

  public V remove(Object key)
  { 
    throw new RuntimeException("not implemented - is stupid");
  }

  public Collection<V>   values()
  { 
    throw new RuntimeException("not implemented - is stupid");
  }

}
