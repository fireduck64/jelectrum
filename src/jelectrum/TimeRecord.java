package jelectrum;

import java.util.TreeMap;
import java.util.Map;
import java.text.DecimalFormat;
import java.io.PrintStream;

public class TimeRecord
{
  private TreeMap<String, Long> times=new TreeMap<String, Long>();

  public synchronized void addTime(long tm, String name)
  {
    Long prev = times.get(name);
    long p = 0;
    if (prev != null) p = prev;

    times.put(name, p + tm);
  }

  public synchronized void printReport(PrintStream out)
  {
    DecimalFormat df = new DecimalFormat("0.000");

    for(Map.Entry<String, Long> me : times.entrySet())
    {
      String name = me.getKey();
      long nanosec = me.getValue();
      double seconds = nanosec / 1e9;
      out.println("  " + name + " - " + df.format(seconds) + " seconds");
    }

  }

  public synchronized void reset()
  {
    times.clear();
  }


}

