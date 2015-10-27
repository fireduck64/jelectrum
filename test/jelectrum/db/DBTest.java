package jelectrum.db;

import org.junit.Assert;
import org.junit.Test;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.DB;
import jelectrum.db.DBTooManyResultsException;

import jelectrum.db.mongo.MongoDB;
import jelectrum.db.lmdb.LMDB;
import jelectrum.db.lobstack.LobstackDB;
import jelectrum.db.level.LevelDB;
import jelectrum.db.memory.MemoryDB;
import jelectrum.db.slopbucket.SlopbucketDB;
import jelectrum.db.jedis.JedisDB;
import jelectrum.Config;
import jelectrum.EventLog;
import org.bitcoinj.core.Sha256Hash;
import java.util.Map;
import java.util.LinkedList;

import java.util.AbstractMap.SimpleEntry;
import com.google.protobuf.ByteString;

public class DBTest
{

  @Test
  public void testMongo() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new MongoDB(conf);
    testDB(db);

  }

  @Test
  public void testLMDB() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LMDB(conf);
    testDB(db);

  }

  @Test
  public void testLobstack() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LobstackDB(null, conf);
    testDB(db);

  }
  @Test
  public void testSlopbucket() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new SlopbucketDB(conf, log);
    testDB(db);

  }


  /*@Test
  public void testLevelDB() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LevelDB(log, conf);
    testDB(db);

  }

  @Test
  public void testLmdbNet() throws Exception
  {
    Config conf = new Config("jelly-lmdbnet.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LevelDB(log, conf);
    testDB(db);

  }*/
 
  @Test
  public void testMemoryDB() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new MemoryDB(conf);
    testDB(db);

  }

  /*@Test
  public void testJedisDB() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new JedisDB(conf);
    testDB(db);

  }*/

  @Test
  public void testRocksDB() throws Exception
  {
    Config conf = new Config("jelly-test.conf");
    EventLog log =new EventLog(System.out);

    DB db = new jelectrum.db.rocksdb.JRocksDB(conf, log);
    testDB(db);
  }





  public static void testDB(DB db)
    throws Exception
  {
    DBMap map = db.openMap("db_test_map");
    DBMapSet set = db.openMapSet("db_test_map_set");

    testDBMap(map);
    testDBMapSet(set);

  }

  public static void testDBMap(DBMap map)
  {
    testMapGrow(map);
    testMapZero(map);
    testMapNull(map);
    testMapContains(map);
    testShortKey(map);

  }


  public static void testShortKey(DBMap map)
  {
      ByteString str = TestUtil.randomByteString(100);
      map.put("", str);
      Assert.assertEquals(100, map.get("").size());
      Assert.assertEquals(str, map.get(""));

  }

  public static void testMapNull(DBMap map)
  {
    Assert.assertNull(map.get("thingthatneverexists"));
  }
  public static void testMapContains(DBMap map)
  {
    
    ByteString str = TestUtil.randomByteString(100);
    Assert.assertFalse(map.containsKey(str + "/nono"));

    map.put(str+"/yes", TestUtil.randomByteString(1024));

    Assert.assertTrue(map.containsKey(str + "/yes"));


  }
  public static void testMapZero(DBMap map)
  {
    map.put("zero", TestUtil.randomByteString(0));
    Assert.assertEquals(0, map.get("zero").size());
  }
  public static void testMapGrow(DBMap map)
  {
   
    int x = 1;
    while(x < 10485760)
    {
      ByteString str = TestUtil.randomByteString(x);
      map.put("grow", str);
      Assert.assertEquals(x, map.get("grow").size());
      Assert.assertEquals(str, map.get("grow"));

      x = x * 2;
    }

  }

  public static void testDBMapSet(DBMapSet set)
  {
    testSetBasic(set);    
    testSetBulk(set);
    testSetPrefix(set);
    testSetPrefixLimit(set);
    

  }

  public static void testSetBasic(DBMapSet set)
  {
    String prefix=TestUtil.randomHash().toString().substring(0,8);

    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "a",TestUtil.randomHash());

    Assert.assertEquals(3, set.getSet(prefix+"a", 10000).size());

  }
  public static void testSetPrefix(DBMapSet set)
  {
    String prefix=TestUtil.randomHash().toString().substring(0,8);

    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "ab",TestUtil.randomHash());
    set.add(prefix + "ab",TestUtil.randomHash());
    set.add(prefix + "ab",TestUtil.randomHash());
    set.add(prefix + "ab",TestUtil.randomHash());

    Assert.assertEquals(2, set.getSet(prefix+"a", 10000).size());
    Assert.assertEquals(4, set.getSet(prefix+"ab", 10000).size());

  }

  public static void testSetPrefixLimit(DBMapSet set)
  {
    String prefix=TestUtil.randomHash().toString().substring(0,8);
    for(int i=0; i<100; i++)
    {
      set.add(prefix, TestUtil.randomHash());
    }
    Assert.assertEquals(100, set.getSet(prefix, 100).size());
    set.add(prefix, TestUtil.randomHash());
    try
    {
      set.getSet(prefix, 100);
      Assert.fail();
    }
    catch(DBTooManyResultsException e)
    {
    }




  }
  public static void testSetBulk(DBMapSet set)
  {
    String prefix=TestUtil.randomHash().toString().substring(0,8);

    LinkedList<Map.Entry<String, Sha256Hash> > lst = new LinkedList<>();

    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"a", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"b", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"b", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"b", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"b", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"c", TestUtil.randomHash()));
    lst.add(new SimpleEntry<String, Sha256Hash>(prefix+"c", TestUtil.randomHash()));

    set.addAll(lst);

    Assert.assertEquals(1, set.getSet(prefix+"a", 1000).size());
    Assert.assertEquals(4, set.getSet(prefix+"b", 1000).size());
    Assert.assertEquals(2, set.getSet(prefix+"c", 1000).size());

  }

}
