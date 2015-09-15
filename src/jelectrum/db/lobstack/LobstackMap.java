package jelectrum.db.lobstack;

import java.util.Map;
import java.util.TreeMap;
import java.nio.ByteBuffer;

import jelectrum.db.DBMap;
import lobstack.Lobstack;

import com.google.protobuf.ByteString;


public class LobstackMap extends DBMap
{
  Lobstack stack;

  public LobstackMap(Lobstack stack)
  {
    this.stack = stack;
  }

  public ByteString get(String key)
  {
    try
    {
      ByteBuffer bb = stack.get(key);
      if (bb == null) return null;
      else return ByteString.copyFrom(bb);
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public void put(String key, ByteString value)
  {
    try
    {
      stack.put(key, ByteBuffer.wrap(value.toByteArray()));
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }
  }

  public void putAll(Map<String, ByteString> m)
  {
    try
    {
      TreeMap<String, ByteBuffer> pm = new TreeMap<>();
      for(Map.Entry<String, ByteString> me : m.entrySet())
      {
        pm.put(me.getKey(), ByteBuffer.wrap(me.getValue().toByteArray()));
      }
      stack.putAll(pm);
    }
    catch(java.io.IOException e)
    {
      throw new RuntimeException(e);
    }


  }


}
