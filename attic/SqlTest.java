import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jelectrum.Config;
import jelectrum.DB;
import jelectrum.SqlMap;
import jelectrum.MapSet;
import jelectrum.SqlMapSet;
import jelectrum.BandingMap;
import jelectrum.BandingMapSet;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import java.util.concurrent.Semaphore;

import com.google.bitcoin.core.Sha256Hash;



public class SqlTest
{

  @BeforeClass
  public static void setupDB()
    throws Exception
  {
    Config config = new Config("jelly.conf");
        DB.openConnectionPool(
            "jelectrum_db",
            config.get("sql_db_driver"),
            config.get("sql_db_uri"),
            config.get("sql_db_username"),
            config.get("sql_db_password"),
            config.getInt("sql_db_conns"),
            16);
  }

  @Test
  public void testBasicMap()
  {
    SqlMap<String, String> map = new SqlMap("test_map",128);

    String first = "z3243111";
    String second = "afgggggg";

    map.clear();
    Assert.assertEquals(0,map.size());
    map.put("a", first);

    Assert.assertEquals(first, map.get("a"));

    map.put("a", second);

    Assert.assertEquals(second, map.get("a"));
  }

  @Test
  public void testBasicMapSet()
  {
    SqlMapSet<String> mapset = new SqlMapSet("test_map_set",128);

    Random rnd = new Random();
    String key = "z" + rnd.nextLong();

    Assert.assertEquals(0, mapset.getSet(key).size());

    mapset.add(key, new Sha256Hash("470336b0556e8ffe214a6360a24da7b902c64f4a1c25d22bf81a01034c46d388"));

    Assert.assertEquals(1, mapset.getSet(key).size());

    mapset.add(key, new Sha256Hash("470336b0556e8ffe214a6360a24da7b902c64f4a1c25d22bf81a01034c46d388"));
    Assert.assertEquals(1, mapset.getSet(key).size());

  }

  @Test
  public void testMapPutAll()
  {
    SqlMap<String, String> map = new SqlMap("test_map",128);

    map.clear();
    Assert.assertEquals(0,map.size());

    map.put("15","meow");
    TreeMap<String, String> in = new TreeMap<String, String>();
    for(int i=0; i<1024; i++)
    {
      in.put("" + i, "" + i + "," + i + "," + i);
    }
    map.putAll(in);
    Assert.assertEquals(1024, map.size());
    Assert.assertEquals("15,15,15", map.get("15"));
  }
  @Test
  public void testMapPutAllSeqence()
  {
    SqlMap<String, String> map = new SqlMap("test_map",128);

    map.clear();
    Assert.assertEquals(0,map.size());

    for(int i=0; i<1024; i++)
    {
      map.put("" + i, "" + i + "," + i + "," + i);

    }

    Assert.assertEquals(1024, map.size());
    Assert.assertEquals("15,15,15", map.get("15"));

  }

  @Test
  public void testBandingMap()
    throws Exception
  {
    Map<String, String> map = new BandingMap<String,String>(new SqlMap<String, String>("test_map",128),200);

    map.clear();

    threadedSaveThings(map, 1024);

  }
  @Test
  public void testBandingMapSet()
    throws Exception
  {
    MapSet<String, Sha256Hash> map = new BandingMapSet<String,Sha256Hash>(new SqlMapSet<String>("test_map_set",128),200);


    threadedSaveThingsSet(map, 1024);

  }


  private void threadedSaveThings(Map<String, String> map, int count)
    throws Exception
  {
    Semaphore sem = new Semaphore(0);

    Assert.assertEquals(0, map.size());
    for(int i=0; i<count; i++)
    {
      new SaveThread(map, sem).start();
    }
    sem.acquire(count);
    Assert.assertEquals(count, map.size());

  }
 private void threadedSaveThingsSet(MapSet<String, Sha256Hash> map, int count)
    throws Exception
  {
    Semaphore sem = new Semaphore(0);

    for(int i=0; i<count; i++)
    {
      new SaveThreadSet(map, sem).start();
    }
    sem.acquire(count);

  }


  public class SaveThread extends Thread
  {
    private Random rnd;
    private Map<String, String> map;
    private Semaphore sem;

    public SaveThread(Map<String, String> map, Semaphore sem)
    {
      this.map = map;
      this.sem = sem;

    }

    public void run()
    {
      rnd = new Random();
      String key = "k-" + rnd.nextLong();
      String value = "" + rnd.nextLong();
      map.put(key, value);

      sem.release();
      
    }

  }
  public class SaveThreadSet extends Thread
  {
    private Random rnd;
    private MapSet<String, Sha256Hash> map;
    private Semaphore sem;

    public SaveThreadSet(MapSet<String, Sha256Hash> map, Semaphore sem)
    {
      this.map = map;
      this.sem = sem;

    }

    public void run()
    {
      rnd = new Random();
      String key = "k-" + rnd.nextLong();

      byte[] b = new byte[32];
      rnd.nextBytes(b);
      Sha256Hash hash = new Sha256Hash(b);
      map.add(key, hash);

      sem.release();
      
    }

  }




}
