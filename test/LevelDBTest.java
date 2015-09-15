import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jelectrum.Config;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.nio.ByteBuffer;

import java.util.concurrent.Semaphore;

import com.google.bitcoin.core.Sha256Hash;
import jelectrum.Jelectrum;
import java.util.Set;
import java.util.LinkedList;
import java.util.AbstractMap.SimpleEntry;

import jelectrum.db.level.*;
import com.google.protobuf.ByteString;


public class LevelDBTest
{

  @Test
  public void testBasicPutGet()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    c.put("test_a", randomBytes(1024));

    ByteString b = c.get("test_a");

    Assert.assertEquals(1024, b.size());

  }
  @Test
  public void testBasicGetNoExist()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    ByteString b = c.get("test_doesnotexist");

    Assert.assertNull(b);

  }
  @Test
  public void testStoreNull()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    c.put("test_zero", randomBytes(0));
    c.put("test_null", null);

    Assert.assertNull(c.get("test_null"));
    Assert.assertNull(c.get("test_zero"));

  }

  @Test
  public void testGetPrefix()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;


    c.put("test_prd", randomBytes(27));
    c.put("test_pre_a", null);
    c.put("test_pre_c", randomBytes(1000));
    c.put("test_pre_z", randomBytes(0));
    c.put("test_prf", randomBytes(27));

    Map<String,ByteString> m = c.getByPrefix("test_pre_");

    Assert.assertEquals(3, m.size());
    Assert.assertEquals(1000, m.get("test_pre_c").size());
    Assert.assertNull(m.get("test_pre_a"));
    Assert.assertNull(m.get("test_pre_z"));

  }

  @Test
  public void testPutAll()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    Map<String,ByteString> m = new TreeMap<String, ByteString>();

    for(int i=0; i<2048; i++)
    {
      m.put("test_putall_" + i, randomBytes(1000));
    }

    c.putAll(m);

    Map<String,ByteString> m2 = c.getByPrefix("test_putall_");

    Assert.assertEquals(m.size(), m2.size());
    Assert.assertEquals(m, m2);



  }

  @Test
  public void testMapSet()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    Random rnd=new Random();

    LevelDBMapSet ms = new LevelDBMapSet(c, "testmapset");

    String key = "key" + rnd.nextLong();

    Sha256Hash h = TestUtil.randomHash(); 
    ms.add(key, h);

    Set<Sha256Hash> set;
    set = ms.getSet(key);
    Assert.assertEquals(1, set.size());
    Assert.assertTrue(set.contains(h));


    h = TestUtil.randomHash();
    ms.add(key, h);

    set = ms.getSet(key);
    Assert.assertEquals(2, set.size());
    Assert.assertTrue(set.contains(h));


    LinkedList<Map.Entry<String, Sha256Hash>> lst = new LinkedList<Map.Entry<String, Sha256Hash>>();
    for(int i=0; i<25; i++)
    {
      lst.add(new SimpleEntry<String, Sha256Hash>(key, TestUtil.randomHash() ));
    }
    ms.addAll(lst);

    set = ms.getSet(key);
    Assert.assertEquals(27, set.size());




  }

  @Test
  public void testMapSetClosePrefix()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(new Jelectrum(config), config);
    c.throw_on_error=true;

    Random rnd=new Random();

    LevelDBMapSet ms = new LevelDBMapSet(c, "testmapset");

    String key = "key" + rnd.nextLong();
    String key_a = key + "a";
    String key_b = key + "b";
    
    for(int i=0; i<1024; i++)
    {
      ms.add(key_a,TestUtil.randomHash());
      ms.add(key_b,TestUtil.randomHash());
    }

    Assert.assertEquals(1024, ms.getSet(key_a).size());
    Assert.assertEquals(1024, ms.getSet(key_b).size());





  }


 
 
  public ByteString randomBytes(int len)
  {
    Random rnd = new Random();
    byte[] b = new byte[len];

    rnd.nextBytes(b);

    return ByteString.copyFrom(b);

  }




}
