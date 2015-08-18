package jelectrum;
import java.io.File;

public class FreeSpaceWatcher extends Thread
{
  File location;
  Jelectrum jelly;
  long min_space;

  public FreeSpaceWatcher(File location, Jelectrum jelly, long min_space)
  {
    setName("FreeSpaceWatcher@" + location);
    setDaemon(true);

    this.location = location;
    this.jelly = jelly;
    this.min_space = min_space;

  }

  public void run()
  {
    while(true)
    {
      try
      {
        long space = location.getUsableSpace();
        if (space < min_space)
        {
          jelly.getEventLog().alarm("FreeSpaceWacher: low on space in " + location);
          System.exit(10);
         
        }
        sleep(20000);

      }
      catch(Throwable t)
      {
        jelly.getEventLog().alarm("FreeSpaceWacher: " + t);
        t.printStackTrace();
      }

    }

  }
}
