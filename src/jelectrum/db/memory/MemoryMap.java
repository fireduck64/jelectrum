package jelectrum.db.memory;

import jelectrum.db.DBMap; 
import com.google.protobuf.ByteString;

import java.util.TreeMap;


public class MemoryMap extends DBMap
{
  private TreeMap<String, ByteString> m=new TreeMap<>();

  public synchronized ByteString get(String key)
  {
    return m.get(key);
  }

  public synchronized void put(String key, ByteString value)
  {
    m.put(key, value);
  }


}


