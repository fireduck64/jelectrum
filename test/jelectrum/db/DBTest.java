package jelectrum.db;

import org.junit.Assert;
import org.junit.Test;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.DB;

import jelectrum.db.mongo.MongoDB;
import jelectrum.db.lmdb.LMDB;
import jelectrum.db.lobstack.LobstackDB;
import jelectrum.Config;
import jelectrum.EventLog;
import com.google.bitcoin.core.Sha256Hash;
import java.util.Map;
import java.util.LinkedList;

import java.util.AbstractMap.SimpleEntry;
import com.google.protobuf.ByteString;

public class DBTest
{

  @Test
  public void testMongo() throws Exception
  {
    Config conf = new Config("jelly-grind.conf");
    EventLog log =new EventLog(System.out);

    DB db = new MongoDB(conf);
    testDB(db);

  }

  @Test
  public void testLMDB() throws Exception
  {
    Config conf = new Config("jelly-grind.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LMDB(conf);
    testDB(db);

  }

  @Test
  public void testLobstack() throws Exception
  {
    Config conf = new Config("jelly-grind.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LobstackDB(null, conf);
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
    

  }

  public static void testSetBasic(DBMapSet set)
  {
    String prefix=TestUtil.randomHash().toString().substring(0,8);

    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "a",TestUtil.randomHash());
    set.add(prefix + "a",TestUtil.randomHash());

    Assert.assertEquals(3, set.getSet(prefix+"a").size());

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

    Assert.assertEquals(2, set.getSet(prefix+"a").size());
    Assert.assertEquals(4, set.getSet(prefix+"ab").size());

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

    Assert.assertEquals(1, set.getSet(prefix+"a").size());
    Assert.assertEquals(4, set.getSet(prefix+"b").size());
    Assert.assertEquals(2, set.getSet(prefix+"c").size());

  }

}
