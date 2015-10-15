package jelectrum.db.lobstack;

import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import jelectrum.db.DBMapSet;

import java.nio.ByteBuffer;
import org.bitcoinj.core.Sha256Hash;
import lobstack.Lobstack;

import jelectrum.db.DBTooManyResultsException;

public class LobstackMapSet extends DBMapSet
{
  private Lobstack stack;

    public LobstackMapSet(Lobstack stack)
    {
      this.stack = stack;
    }

    public Set<Sha256Hash> getSet(String p, int max_results)
    {
      try
      {
        HashSet<Sha256Hash> ret = new HashSet<Sha256Hash>();
        String search = p + "/";
        int len = search.length();
        int count = 0;
        for(String s : stack.getByPrefix(search).keySet())
        {
          ret.add(new Sha256Hash(s.substring(len)));
          count ++;
          if (count > max_results) throw new DBTooManyResultsException();
        }
        return ret;
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }

    public void addAll(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      try
      {
        Map<String, ByteBuffer> write_map = new TreeMap<String, ByteBuffer>();

        byte[] buff=new byte[0];
        ByteBuffer bb = ByteBuffer.wrap(buff);

        for(Map.Entry<String, Sha256Hash> me : lst)
        {
          write_map.put(me.getKey() + "/" + me.getValue(), bb);
        }
        
        stack.putAll(write_map);
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}

    }

    public void add(String p, Sha256Hash v)
    {
      try
      {
        byte[] buff=new byte[0];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        stack.put(p + "/" + v, bb);

      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }
}
