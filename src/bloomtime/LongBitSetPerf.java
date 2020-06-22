package bloomtime;

import duckutil.TimeRecord;
import java.io.File;
import java.util.Random;
import java.text.DecimalFormat;

public class LongBitSetPerf
{
  public static void main(String args[])
    throws Exception
  {
    String name = args[0];
    String base = args[1];
    new LongBitSetPerf(name, base);
  }

  final long file_len=10L *1000L *1000L *1000L;
  final long bit_len=file_len * 8L;

  Random rnd;
  String base;

  public LongBitSetPerf(String name, String base)
    throws Exception
  {

    this.base = base;

    rnd = new Random();


    new File(base).mkdirs();
    if (name.equals("direct"))
    {
      testRun(new LongBitSetDirect(setupLongFile("direct")), "direct");
    }
    if (name.equals("simple"))
    {
      testRun(new LongBitSetSimple(setupLongFile("simple")), "simple");
    }
    if (name.equals("threaded"))
    {
      testRun(new LongBitSetThreaded(setupLongFile("threaded"), bit_len), "threaded");
    }
    if (name.equals("fancy"))
    {
      testRun(new LongBitSetFancy(new File(base + "/fancylog"),setupLongFile("fancy"), bit_len), "fancy");
    }

  }

  private LongFile setupLongFile(String name)
    throws Exception
  {
    System.gc();
    File test_file = new File(base + "/" + name);
    test_file.delete();
    return new LongMappedBuffer(test_file, file_len);

  }



  private void testRun(LongBitSet lbs, String name)
  {

    System.out.println("------- Starting " + name + " --------");
    TimeRecord tr=new TimeRecord();
    TimeRecord.setSharedRecord(tr);
    long t1 = System.nanoTime();
    int count = 100 * 1000 * 1000;

    for(int i=0; i<count; i++)
    {
      long v = BloomUtil.nextLong(rnd, bit_len);
      lbs.setBit(v);
      if (i % 10000 == 0) lbs.flush();
    }
    lbs.cleanup();

    TimeRecord.record(t1, "total_" + name);
    double seconds = System.nanoTime() - t1;
    seconds = seconds / 1e9;
    double rate = count / seconds;
    DecimalFormat df = new DecimalFormat("0.0");

    tr.printReport(System.out);
    System.out.println("Run " + name + " rate " + df.format(rate) + " bits/sec");

  }


}
