package jelectrum.db;

import org.junit.Assert;
import org.junit.Test;

import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;
import jelectrum.db.DB;

import jelectrum.db.mongo.MongoDB;
import jelectrum.db.lmdb.LMDB;
import jelectrum.Config;
import jelectrum.EventLog;

import com.google.protobuf.ByteString;

public class DBTest
{

  @Test
  public void testMongo() throws Exception
  {
    Config conf = new Config("jelly.conf");
    EventLog log =new EventLog(System.out);

    DB db = new MongoDB(conf);
    testDB(db);

  }
  @Test
  public void testLMDB() throws Exception
  {
    Config conf = new Config("jelly.conf");
    EventLog log =new EventLog(System.out);

    DB db = new LMDB(conf);
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
    

  }

}
