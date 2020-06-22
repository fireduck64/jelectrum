package bloomtime;

import duckutil.TimeRecord;
import java.util.Random;
import org.junit.Assert;

public class BloomUtil
{

  public static long nextLong(Random rnd, long max)
  {
    long t1 = System.nanoTime();
    Assert.assertEquals(0, max % 1000000);
    int segments = (int)(max / 1000000);
    long seg = rnd.nextInt(segments);
    long v = rnd.nextInt(1000000);

    long next = v + seg * 1000000L;
    TimeRecord.record(t1, "bloomutil_nextlong");

    return next;


  }

}
