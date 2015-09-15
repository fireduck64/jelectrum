package jelectrum.db.level;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

import org.junit.Assert;

import java.nio.ByteBuffer;
import com.google.bitcoin.core.Sha256Hash;
import jelectrum.db.DBMapSet;
import com.google.protobuf.ByteString;

public class LevelDBMapSet extends DBMapSet
{
  private LevelNetClient c;
  private String prefix;

    public LevelDBMapSet(LevelNetClient c, String prefix)
    {
      this.c = c;
      this.prefix=prefix + "/";
    }

    public Set<Sha256Hash> getSet(String p)
    {
      HashSet<Sha256Hash> ret = new HashSet<Sha256Hash>();
      String first=prefix + p;
      int len = first.length() + 1;
      for(String s : c.getByPrefix(first).keySet())
      {
        Assert.assertEquals(first, s.substring(0, first.length()));
        ret.add(new Sha256Hash(s.substring(len)));
      }
      return ret;
    }

    public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      Map<String, ByteString> write_map = new TreeMap<String, ByteString>();

      for(Map.Entry<String, Sha256Hash> me : lst)
      {
        write_map.put(prefix + me.getKey() + "/" + me.getValue(), null);
      }
      
      c.putAll(write_map);

    }

    public void add(String p, Sha256Hash v)
    {
      c.put(prefix + p + "/" + v, null);
    }
}
