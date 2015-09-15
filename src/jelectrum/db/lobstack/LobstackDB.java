package jelectrum.db.lobstack;

import jelectrum.db.DB;
import jelectrum.db.DBMap;
import jelectrum.db.DBMapSet;

import java.util.LinkedList;
import java.io.PrintStream;
import java.io.File;

import lobstack.Lobstack;
import jelectrum.Jelectrum;
import jelectrum.Config;
import java.io.FileOutputStream;
import java.util.TreeMap;


public class LobstackDB extends DB
{
  protected boolean compress;

  protected LinkedList<Lobstack> stack_list;
  protected PrintStream cleanup_log;
  protected Jelectrum jelly;

  public LobstackDB(Jelectrum jelly, Config config)
    throws Exception
  {
    super(config);

    this.jelly = jelly;

    compress=false;

    stack_list = new LinkedList<Lobstack>();
    config.require("lobstack_path");
    config.require("lobstack_minfree_gb");
    if (config.isSet("lobstack_compress")) compress = config.getBoolean("lobstack_compress");


    File path = new File(conf.get("lobstack_path"));
    
    path.mkdirs();

    open();

    if (jelly != null)
    {
      cleanup_log = new PrintStream(new FileOutputStream("lobstack.log", true));
      new LobstackMaintThread().start();
    }

  }


  protected DBMap openMap(String name)
    throws java.io.IOException
  {
    Lobstack l = new Lobstack(new File(conf.get("lobstack_path")), name, compress, 1);
    stack_list.add(l);
    return new LobstackMap(l);

  }

  protected DBMapSet openMapSet(String name)
    throws java.io.IOException
  {
    
    Lobstack l = new Lobstack(new File(conf.get("lobstack_path")), name, compress, 1);
    stack_list.add(l);
    return new LobstackMapSet(l);
  }

    public class LobstackMaintThread extends Thread
    { 
      public LobstackMaintThread()
      { 
        setName("LobstackMaintThread");
        setDaemon(true);
      }

      public void run()
      { 
        TreeMap<String, Long> check_delay_map = new TreeMap<String, Long>();

        while(true)
        { 
          try
          { 
            boolean done_something = false;

            for(Lobstack ls : stack_list)
            { 
              String name = ls.getName();
              int depth=4;
              double target=0.50;
              long max_size = 1024L * 1024L * 1024L;
              if (jelly.getSpaceLimited())
              { 
                name = "limited-" + name;
                depth=16;
                target=0.95;
                max_size=4L * 1024L * 1024L * 1024L;
              }
              if ((!check_delay_map.containsKey(name)) || (check_delay_map.get(name) < System.currentTimeMillis()))
              { 
                if (ls.cleanup(depth, target, max_size, cleanup_log))
                { 
                  done_something=true;
                }
                else
                { 
                  check_delay_map.put(name, 60L * 60L * 1000L + System.currentTimeMillis());
                }

              }
            }
            if (!done_something)
            { 
              //cleanup_log.println("Sleeping");
              sleep(5L * 1000L);
            }
          }
          catch(Exception e)
          { 
            e.printStackTrace();
          }
        }
      }
  }


}
