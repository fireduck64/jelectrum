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
import jelectrum.LevelNetClient;


public class LevelDBTest
{

  @Test
  public void testBasicPutGet()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(config);
    c.throw_on_error=true;

    c.put("test_a", randomBytes(1024));

    ByteBuffer b = c.get("test_a");

    Assert.assertEquals(1024, b.capacity());

  }
  @Test
  public void testBasicGetNoExist()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(config);
    c.throw_on_error=true;

    ByteBuffer b = c.get("test_doesnotexist");

    Assert.assertNull(b);

  }
  @Test
  public void testStoreNull()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(config);
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
    LevelNetClient c = new LevelNetClient(config);
    c.throw_on_error=true;


    c.put("test_prd", randomBytes(27));
    c.put("test_pre_a", null);
    c.put("test_pre_c", randomBytes(1000));
    c.put("test_pre_z", randomBytes(0));
    c.put("test_prf", randomBytes(27));

    Map<String,ByteBuffer> m = c.getByPrefix("test_pre_");

    Assert.assertEquals(3, m.size());
    Assert.assertEquals(1000, m.get("test_pre_c").capacity());
    Assert.assertNull(m.get("test_pre_a"));
    Assert.assertNull(m.get("test_pre_z"));

  }

  @Test
  public void testPutAll()
    throws Exception
  {
    Config config = new Config("jelly.conf");
    LevelNetClient c = new LevelNetClient(config);
    c.throw_on_error=true;

    Map<String,ByteBuffer> m = new TreeMap<String, ByteBuffer>();

    for(int i=0; i<2048; i++)
    {
      m.put("test_putall_" + i, randomBytes(1000));
    }

    c.putAll(m);

    Map<String,ByteBuffer> m2 = c.getByPrefix("test_putall_");

    Assert.assertEquals(m.size(), m2.size());



  }


 
 
  public ByteBuffer randomBytes(int len)
  {
    Random rnd = new Random();
    byte[] b = new byte[len];

    rnd.nextBytes(b);

    return ByteBuffer.wrap(b);

  }




}
