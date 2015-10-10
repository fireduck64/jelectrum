
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;

import bloomtime.Bloomtime;

import java.io.File;
import java.util.BitSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.LinkedList;
import com.google.protobuf.ByteString;
import jelectrum.BloomLayerCake;

public class BloomtimeTest
{
  private static Bloomtime bloom;

  @BeforeClass
  public static void loadMap()
    throws Exception
  {
    new File("/var/ssd/clash/test").mkdirs();

    File f = new File("/var/ssd/clash/test/bloomtime-test");
    f.delete();
    bloom = new Bloomtime(f, 8192, 1048576, 4); 
  }

  @Test
  public void testDetRandom()
  {
    ByteString b = TestUtil.randomByteString();
    Assert.assertEquals(bloom.getHashIndexes(b), bloom.getHashIndexes(b));
    Assert.assertEquals(4, bloom.getHashIndexes(b).size());
    System.out.println(bloom.getHashIndexes(b));
  }

  @Test
  public void testConsistentRandom()
  {
    ByteString b = ByteString.copyFrom(new String("this is a test string").getBytes());
    Assert.assertEquals(bloom.getHashIndexes(b), bloom.getHashIndexes(b));
    Assert.assertEquals(4, bloom.getHashIndexes(b).size());

    System.out.println(bloom.getHashIndexes(b));
    Set<Integer> v = bloom.getHashIndexes(b);

    Assert.assertTrue(v.contains(253540));
    Assert.assertTrue(v.contains(669940));
    Assert.assertTrue(v.contains(970472));
    Assert.assertTrue(v.contains(526572));

  }


  @Test
  public void testBasicBloom()
  {
    ByteString b = TestUtil.randomByteString();

    bloom.saveEntry(901, b);

    Set<Integer> v = bloom.getMatchingSlices(b);

    System.out.println(v);

    Assert.assertTrue( v.contains(901));

  }
  @Test
  public void testSliceRange()
  {
    ByteString b = TestUtil.randomByteString();

    bloom.saveEntry(800, b);

    Set<Integer> v = bloom.getMatchingSlices(b);
    Assert.assertTrue( v.contains(800));

    Assert.assertEquals(0, bloom.getMatchingSlices(b,0,800).size());
    Assert.assertEquals(1, bloom.getMatchingSlices(b,800,808).size());
    Assert.assertEquals(1, bloom.getMatchingSlices(b,801,808).size());
    Assert.assertEquals(0, bloom.getMatchingSlices(b,808,8192).size());

    Assert.assertTrue(bloom.getMatchingSlices(b,800,808).contains(800));
    Assert.assertTrue(bloom.getMatchingSlices(b,805,808).contains(800));
  }


  @Test
  public void testFullerBloom()
  {
    Random rnd = new Random();

    HashMap<ByteString, Integer> correct_map = new HashMap<>();
    for(int i=0; i<10000; i++)
    {
      int slice = rnd.nextInt(8192);
      ByteString b = TestUtil.randomByteString();
      bloom.saveEntry(slice,b);
      correct_map.put(b, slice);
    }

    Assert.assertEquals(10000, correct_map.size());
    for(Map.Entry<ByteString, Integer> me : correct_map.entrySet())
    {
      ByteString b = me.getKey();
      int slice = me.getValue();
      Set<Integer> v = bloom.getMatchingSlices(b);
      Assert.assertTrue( v.contains(slice));
    }

    ByteString b = TestUtil.randomByteString();
    bloom.saveEntry(901, b);
    Set<Integer> v = bloom.getMatchingSlices(b);
    System.out.println(v);
    Assert.assertTrue( v.contains(901));
  }

  @Test
  public void testLayerCake()
    throws Exception
  {
    BloomLayerCake cake = new BloomLayerCake(new File("/var/ssd/clash/test/bloom"));
    Random rnd = new Random();
    for(int i=0; i<75000; i+=rnd.nextInt(8192)+1)
    {
      LinkedList<String> addrs = new LinkedList<String>();
      for(int j=0; j<8; j++) addrs.add(TestUtil.randomHash().toString());
      cake.addAddresses(i, addrs);
    }
    LinkedList<String> addrs = new LinkedList<String>();
    String addr = TestUtil.randomHash().toString();
    addrs.add(addr);
    cake.addAddresses(5, addrs);
    cake.addAddresses(3506, addrs);
    cake.addAddresses(740021, addrs);
    cake.flush();

    System.out.println("--------test layer");

    Set<Integer> v = cake.getBlockHeightsForAddress(addr);
    Assert.assertTrue(v.size() >= 3);
    Assert.assertTrue(v.contains(5));
    Assert.assertTrue(v.contains(3506));
    Assert.assertTrue(v.contains(740021));

    System.out.println("--------test layer");

  }

}
