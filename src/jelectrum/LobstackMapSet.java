package jelectrum;

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

import java.nio.ByteBuffer;
import com.google.bitcoin.core.Sha256Hash;
import lobstack.Lobstack;

public class LobstackMapSet
{
  private Lobstack stack;

    public LobstackMapSet(Lobstack stack)
    {
      this.stack = stack;
    }

    public Set<Sha256Hash> getSet(String p)
    {
      try
      {
        HashSet<Sha256Hash> ret = new HashSet<Sha256Hash>();
        String first=p;
        int len = first.length() + 1;
        for(String s : stack.getByPrefix(p).keySet())
        {
          ret.add(new Sha256Hash(s.substring(len)));
        }
        return ret;
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }

    public void putList(Collection<Map.Entry<String, Sha256Hash> > lst)
    {
      try
      {
        Map<String, ByteBuffer> write_map = new TreeMap<String, ByteBuffer>();

        byte[] buff=new byte[1];
        ByteBuffer bb = ByteBuffer.wrap(buff);

        for(Map.Entry<String, Sha256Hash> me : lst)
        {
          write_map.put(me.getKey() + "/" + me.getValue(), bb);
        }
        
        stack.putAll(write_map);
      }
      catch(java.io.IOException e){throw new RuntimeException(e);}

    }

    public void put(String p, Sha256Hash v)
    {
      try
      {
        byte[] buff=new byte[1];
        ByteBuffer bb = ByteBuffer.wrap(buff);
        stack.put(p + "/" + v, bb);

      }
      catch(java.io.IOException e){throw new RuntimeException(e);}
    }
}
