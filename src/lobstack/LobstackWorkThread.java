package lobstack;


import java.util.concurrent.SynchronousQueue;

import jelectrum.SimpleFuture;

public class LobstackWorkThread extends Thread
{
  private SynchronousQueue<WorkUnit> queue;

  public LobstackWorkThread(SynchronousQueue<WorkUnit> queue)
  {
    this.queue = queue;
    setName("LobstackWorkThread");
    setDaemon(true);


  }

  public void run()
  {
    while(true)
    {
      SimpleFuture<NodeEntry> return_node = null;
      try
      {
        WorkUnit wu = queue.take();
        if (wu.mode.equals("PUT"))
        {
          return_node = wu.return_entry;

          NodeEntry ne = wu.node.putAll(wu.stack, wu.save_entries, wu.put_map);

          return_node.setResult(ne);
        }
        else if (wu.mode.equals("REPOSITION"))
        {
          return_node = wu.return_entry;
          NodeEntry ne = wu.node.reposition(wu.stack, wu.save_entries, wu.min_file);
          return_node.setResult(ne);
        }
        else if (wu.mode.equals("ESTIMATE_REPOSITION"))
        {
          long sz = wu.node.estimateReposition(wu.stack, wu.min_file);

          wu.estimate.setResult(sz);

        }

      }
      catch(Throwable t)
      {
        if (return_node != null)
        {
          t.printStackTrace();
          return_node.setException(new RuntimeException(t));
        }
        else
        {
          t.printStackTrace();
        }


      }

    }

  }
  


}
