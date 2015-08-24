
import org.junit.Test;
import org.junit.Assert;

import java.util.Random;
import lobstack.ZUtil;

public class ZipTest
{

  @Test
  public void basicZipTest()
  {
    Random rnd = new Random();
    byte[] in = new byte[3421];
    rnd.nextBytes(in);

    byte[] cmp = ZUtil.compress(in);
    byte[] out = ZUtil.decompress(cmp);

    Assert.assertArrayEquals(in, out);


  }
  @Test
  public void basicZipTest1()
  {
    Random rnd = new Random();
    byte[] in = new byte[1];
    rnd.nextBytes(in);

    byte[] cmp = ZUtil.compress(in);
    byte[] out = ZUtil.decompress(cmp);

    Assert.assertArrayEquals(in, out);


  }
   @Test
  public void basicZipTest0()
  {
    Random rnd = new Random();
    byte[] in = new byte[0];
    rnd.nextBytes(in);

    byte[] cmp = ZUtil.compress(in);
    byte[] out = ZUtil.decompress(cmp);

    Assert.assertArrayEquals(in, out);


  }
  
  @Test
  public void basicZipLoadTest()
  {
    Random rnd = new Random();

    for(int i=0; i<50000; i++)
    {
      byte[] in = new byte[rnd.nextInt(2048)];
      rnd.nextBytes(in);

      byte[] cmp = ZUtil.compress(in);

      byte[] out = ZUtil.decompress(cmp);

      Assert.assertArrayEquals(in, out);
    }

  }



}
