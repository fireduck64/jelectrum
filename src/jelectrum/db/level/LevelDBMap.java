package jelectrum.db.level;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.Collection;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import java.nio.ByteBuffer;
import org.bitcoinj.core.Sha256Hash;

import jelectrum.db.DBMap;
import com.google.protobuf.ByteString;

public class LevelDBMap extends DBMap
{
  private LevelNetClient c;
  private String prefix;

    public LevelDBMap(LevelNetClient c, String prefix)
    {
      this.c = c;
      this.prefix=prefix + "/" ;
    }

    public ByteString get(String key)
    {
      return c.get(prefix + key);

    }

    public void put(String key, ByteString value) 
    {
      
      c.put(prefix + key, value);

    }

    public void putAll(Map<String,ByteString> m) 
    {
      TreeMap<String, ByteString> pm = new TreeMap<>();
      for(Map.Entry<String, ByteString> me : m.entrySet())
      {
        pm.put( prefix + me.getKey(), me.getValue());
      }
      c.putAll(pm);

    }


}
