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
        boolean limited=false;
        if (space < min_space)
        {
          if (!limited)
          {
            jelly.getEventLog().alarm("FreeSpaceWacher: low on space in " + location);
            jelly.setSpaceLimited(true);
            limited=true;
          }
         
        }
        else
        {
          if (limited)
          {
            jelly.setSpaceLimited(false);
            jelly.getEventLog().alarm("FreeSpaceWacher: ok space in " + location);
            limited=false;
          }
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
