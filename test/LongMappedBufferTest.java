
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;

import bloomtime.LongMappedBuffer;

import java.io.File;
import java.util.BitSet;

public class LongMappedBufferTest
{
  private static LongMappedBuffer map;

  @BeforeClass
  public static void loadMap()
    throws Exception
  {
    new File("/var/ssd/clash/test").mkdirs();

    map = new LongMappedBuffer(new File("/var/ssd/clash/test/longmappedbuffer-test"), 16L*1024L*1024L*1024L);
  }

  @Test
  public void testMapEdge()
  { 

    byte[] target = new byte[65536];

    for(long loc = LongMappedBuffer.MAP_SIZE - 65536 - 100; loc < LongMappedBuffer.MAP_SIZE + 65536 + 100; loc+=913)
    {
      
      byte[] buff = TestUtil.randomBytes(65536);
      map.putBytes(loc, buff);
      map.getBytes(loc, target);
      Assert.assertArrayEquals(buff, target);
    }
  }

  @Test
  public void testBitSet()
  {
    byte[] b = new byte[1];

    long pos = 91;
    map.putBytes(91, b);
    map.getBytes(91, b);
    Assert.assertEquals(0, b[0]);

    for(int i=0; i<8; i++)
    {
      map.setBit(91*8 + i);
      
    }
    map.getBytes(91, b);
    BitSet bs = BitSet.valueOf(b);
    for(int i=0; i<8; i++)
    {
      Assert.assertTrue(bs.get(i));
    }
  }



}
