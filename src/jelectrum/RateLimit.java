package jelectrum;

public class RateLimit
{
  
  private double max_bytes_per_ns;
  private double last_allocated;
  private double buffer_seconds;

  public RateLimit(double maxBytesPerSecond, double buffer_seconds)
  {
    max_bytes_per_ns = maxBytesPerSecond / 1e9;
    this.buffer_seconds = buffer_seconds;

    resetLast();
  }

  private void resetLast()
  {
    last_allocated = Math.max(
      System.nanoTime() - (buffer_seconds * 1e9),
      last_allocated);
  }

  /**
   * Returns true if a rate limit was applied
   */
  public synchronized boolean waitForRate(double bytes)
  {
    resetLast();


    double end_time = last_allocated + (bytes / max_bytes_per_ns);
    double tm = System.nanoTime();

    last_allocated = end_time;
    if (end_time > tm)
    {
      long wait_tm = (long)((end_time - tm) / 1e6);
      try
      {
        Thread.sleep(wait_tm);
      }
      catch(InterruptedException e)
      {
        throw new RuntimeException(e);
      }
      return true;
    }
    return false;

  }


 

}
