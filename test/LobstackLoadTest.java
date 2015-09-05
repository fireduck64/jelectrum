

import lobstack.Lobstack;

import java.nio.ByteBuffer;
import java.util.Random;
import java.io.File;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import java.util.TreeMap;

import java.util.concurrent.Semaphore;
import jelectrum.Config;
import jelectrum.UtxoTrieNode;
import com.google.bitcoin.core.Sha256Hash;
import org.apache.commons.codec.binary.Hex;

public class LobstackLoadTest
{
  private Lobstack openStack()
    throws Exception
  {
    Config c = new Config("jelly.conf");
    return new Lobstack(new File(c.get("lobstack_path")), "test",true,1);
  }
   private Lobstack openStack(String name)
    throws Exception
  {
    Config c = new Config("jelly.conf");
    return new Lobstack(new File(c.get("lobstack_path")), name, true,1);
  }
  
  @Test
  public void testPutAllLarge()
    throws Exception
  {
    Lobstack ls = openStack("test_large"); 

    Random rnd = new Random();


    for(int j=0; j<1024; j++)
    {
      TreeMap<String, ByteBuffer> insert_map = new TreeMap<String, ByteBuffer>();
      for(int i=0; i<1024; i++)
      {
        byte[] key_data = new byte[32];
        rnd.nextBytes(key_data);
        String key = Hex.encodeHexString(key_data);
        byte[] buff = new byte[1024];
        rnd.nextBytes(buff);
        insert_map.put("random_put_all:" + key, ByteBuffer.wrap(buff));
      }
      ls.putAll(insert_map);
      insert_map.clear();
    }

    ls.printTimeReport(System.out);

  }
  



}
