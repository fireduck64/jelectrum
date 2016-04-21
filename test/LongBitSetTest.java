
import java.util.Random;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Set;
import java.util.BitSet;

import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.File;
import bloomtime.LongFile;
import bloomtime.LongMappedBuffer;
import bloomtime.LongBitSetSimple;
import bloomtime.LongBitSetThreaded;
import bloomtime.LongBitSetFancy;
import bloomtime.LongBitSet;
import bloomtime.LongBitSetDirect;

public class LongBitSetTest
{
  Random rnd;
  File test_file;
  LongFile long_file;
  final long file_len=20000000000L;
  final long bit_len=file_len * 8L;

  @Before
  public void setup()
    throws Exception
  {
    new File("/var/ssd/clash/test").mkdirs();
    test_file = new File("/var/ssd/clash/test/longbitset-test");
    test_file.delete();

    long_file = new LongMappedBuffer(test_file, file_len);

    rnd = new Random();
  }

  @After
  public void cleanup()
  {
    test_file.delete();
  }
@Test 
  public void direct()
  {
    LongBitSet set = new LongBitSetDirect(long_file);
    testBitSet(set);
  }

  @Test 
  public void simple()
  {
    LongBitSet set = new LongBitSetSimple(long_file);
    testBitSet(set);
  }
  @Test 
  public void threaded()
  {
    LongBitSet set = new LongBitSetThreaded(long_file, bit_len);
    testBitSet(set);
  }
  @Test 
  public void fancy()
  {
    LongBitSet set = new LongBitSetFancy(new File("/var/ssd/clash/test/longbitset/fancy"), long_file, bit_len);
    testBitSet(set);
  }



  private void testBitSet(LongBitSet lbs)
  {
    ArrayList<Long> added_list = new ArrayList<>();
    TreeSet<Long> added_set = new TreeSet<>();

    for(int i=0; i<100000; i++)
    {
      long n = nextLong();
      added_list.add(n);
      added_set.add(n);
      lbs.setBit(n);
    }

    for(int i=0; i<1000; i++)
    {
      long n = added_list.get(rnd.nextInt(added_list.size()));
      Assert.assertTrue(lbs.getBit(n));
    }

    for(int i=0; i<10000; i++)
    {
      long n = nextLong() / 8 * 8;
      int sz = rnd.nextInt(2000000);
      if (n + sz <= bit_len)
      {
        Set<Long> in_range = added_set.subSet(n, n+sz);
        BitSet bs = lbs.getBitSetRange(n, sz);
        for(Long l : in_range)
        {
          int idx = (int)(l - n);
          Assert.assertTrue(bs.get(idx));
        }
      }
    }

    lbs.cleanup();

    for(int i=0; i<1000; i++)
    {
      long n = added_list.get(rnd.nextInt(added_list.size()));
      Assert.assertTrue(lbs.getBit(n));
    }
  }

  private long nextLong()
  {
    int segments = (int)(bit_len / 1000000L);
    long seg = rnd.nextInt(segments);
    long n = rnd.nextInt(1000000);
    return seg*1000000 + n;

  }
}
