

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

public class LobstackTest
{
  private Lobstack openStack()
    throws Exception
  {
    Config c = new Config("jelly-test.conf");
    return new Lobstack(new File(c.get("lobstack_path")), "test",true);
  }
   private Lobstack openStack(String name)
    throws Exception
  {
    Config c = new Config("jelly-test.conf");
    return new Lobstack(new File(c.get("lobstack_path")), name, true);
  }
  

  @Test
  public void testSimplePut()
    throws Exception
  {
    Lobstack ls = openStack();

    Random rnd = new Random();
    byte[] buff = new byte[2048];
    rnd.nextBytes(buff);


    ls.put("hello", ByteBuffer.wrap(buff));

    Assert.assertNull(ls.get("meow"));

    Assert.assertArrayEquals(buff, ls.get("hello").array());
    //ls.printTree();

  }

  @Test
  public void testSimpleEmpty()
    throws Exception
  {
    Lobstack ls = openStack();

    Random rnd = new Random();
    byte[] buff = new byte[0];


    ls.put("empty", ByteBuffer.wrap(buff));

    Assert.assertNull(ls.get("meow"));

    Assert.assertArrayEquals(buff, ls.get("empty").array());
    //ls.printTree();

  }
  

  @Test
  public void testPutall()
    throws Exception
  {
    Lobstack ls = openStack(); 

    Random rnd = new Random();

    TreeMap<String, ByteBuffer> insert_map = new TreeMap<String, ByteBuffer>();

    for(int i=0; i<2048; i++)
    {
      String key = "" + i;
      while(key.length() < 10) key = "0" + key;
      byte[] buff = new byte[2048];
      rnd.nextBytes(buff);
      insert_map.put("random_put_all:" + key, ByteBuffer.wrap(buff));
    }
    ls.putAll(insert_map);
    //ls.printTree();


    for(String key : insert_map.keySet())
    {
      Assert.assertArrayEquals(key,insert_map.get(key).array(), ls.get(key).array());
    }

  }

  
  @Test
  public void testPutHard()
    throws Exception
  {
    Lobstack ls = openStack();

    Random rnd = new Random();

    TreeMap<String, ByteBuffer> insert_map = new TreeMap<String, ByteBuffer>();

    {
      String key = "";
      for(int i=0; i<10; i++)
      {
        key = key + i;
        byte[] buff = new byte[2048];
        rnd.nextBytes(buff);
        insert_map.put("random_put_hard:" + key, ByteBuffer.wrap(buff));
      }
    }
    ls.putAll(insert_map);
    //ls.printTree();

    for(String key : insert_map.keySet())
    {
      Assert.assertNotNull(ls.get(key));
      Assert.assertArrayEquals(key,insert_map.get(key).array(), ls.get(key).array());
    }
  }

  @Test
  public void testPutRandom()
    throws Exception
  {
    Lobstack ls = openStack();

    Random rnd = new Random();

    TreeMap<String, ByteBuffer> insert_map = new TreeMap<String, ByteBuffer>();

    for(int i=0; i<200; i++)
    {
      String key = "rnd_" + rnd.nextLong();
      byte[] buff = new byte[16*1024];
      rnd.nextBytes(buff);

      ls.put(key, ByteBuffer.wrap(buff));
      insert_map.put(key, ByteBuffer.wrap(buff));
    }

    for(String key : insert_map.keySet())
    {
      Assert.assertNotNull(ls.get(key));
      Assert.assertArrayEquals(key,insert_map.get(key).array(), ls.get(key).array());
    
    }

  }

  @Test
  public void testGetPrefix()
    throws Exception
  {
    Lobstack ls = openStack(); 

    Random rnd = new Random();

    byte[] buff = new byte[16*1024];
    rnd.nextBytes(buff);
    ByteBuffer bb = ByteBuffer.wrap(buff);
    ls.put("prefix_a", bb);
    ls.put("prefix_b", bb);
    ls.put("prefix_b.", bb);
    ls.put("prefix_b3", bb);
    ls.put("prefix_bzzdsasd", bb);
    ls.put("prefix_bzzdsasd2", bb);
    ls.put("prefix_c", bb);
    ls.put("prefix_B", bb);

    Map<String, ByteBuffer> m = ls.getByPrefix("prefix_b");

    Assert.assertEquals(5, m.size());
    Assert.assertTrue(m.containsKey("prefix_b"));

  }



  @Test
  public void testPrintTree()
    throws Exception
  {
    Lobstack ls = openStack();


    //ls.printTree();

  }


  @Test
  public void testSnapshots()
    throws Exception
  {
    Lobstack ls = openStack();

    Random rnd = new Random();
    byte[] buff = new byte[16*1024];
    rnd.nextBytes(buff);
    ByteBuffer bb = ByteBuffer.wrap(buff);

    ls.put("snap_original", bb);
    ls.put("snap_original2", bb);

    Assert.assertEquals(2, ls.getByPrefix("snap_").size());

    long snap = ls.getSnapshot();

    ls.put("snap_crap", bb);
    ls.put("snap_crap2", bb);
    ls.put("snap_crap3", bb);

    Assert.assertEquals(5, ls.getByPrefix("snap_").size());
    Assert.assertEquals(2, ls.getByPrefix("snap_", snap).size());

    ls.revertSnapshot(snap);
    Assert.assertEquals(2, ls.getByPrefix("snap_").size());

 



  }

  
  @Test
  public void testPutAllLarge()
    throws Exception
  {
    Lobstack ls = openStack("test_large"); 

    Random rnd = new Random();

    TreeMap<String, ByteBuffer> insert_map = new TreeMap<String, ByteBuffer>();

    for(int j=0; j<1; j++)
    {
      for(int i=0; i<8192; i++)
      {
        String key = "" + rnd.nextInt(10000000);
        while(key.length() < 8) key = "0" + key;
        byte[] buff = new byte[1024];
        rnd.nextBytes(buff);
        insert_map.put("random_put_all:" + key, ByteBuffer.wrap(buff));
      }
      ls.putAll(insert_map);
      insert_map.clear();
    }

    ls.printTimeReport(System.out);

  }
  
  @Test
  public void testCleanup()
    throws Exception
  {
    Lobstack ls = openStack("test_large"); 
    ls.cleanup(16, 0.75, 256L * 1024L * 1024L);

  }




  @Test
  public void testMultithreaded()
    throws Exception
  {
    Lobstack ls = openStack();

    Semaphore sem = new Semaphore(0);

    for(int i=0; i<4; i++)
    {
      new ActionThread(sem, ls, "write").start();
      new ActionThread(sem, ls, "read").start();
    }

    sem.acquire(8);
    
  }


  public class ActionThread extends Thread
  {
    private String action;
    private Lobstack ls;
    private Semaphore sem;
    
    
   
    public ActionThread(Semaphore sem, Lobstack ls, String action)
    {
      this.sem = sem;
      this.ls = ls;
      this.action = action;
    }


    public void run()
    {
      try
      {
        if (action.equals("read"))
        {
          for(int i=0; i<25; i++)
          {
            int sz = ls.getByPrefix("").size();
            //System.out.println("Reader read: " + sz);
          }
        }
        else if (action.equals("write"))
        {
          Random rnd = new Random();

          for(int i=0; i<25; i++)
          {
            byte[] buff = new byte[1024];
            rnd.nextBytes(buff);
            ByteBuffer bb = ByteBuffer.wrap(buff);
  
            ls.put("thread_" + rnd.nextLong(), bb);
          }
        }

      }
      catch(Exception e)
      {
        e.printStackTrace();
      }
      finally
      {
        sem.release();
      }





    }
  
  }



}
