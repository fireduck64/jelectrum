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
        return_node = wu.return_entry;

        NodeEntry ne = wu.node.putAll(wu.stack, wu.save_entries, wu.put_map);

        return_node.setResult(ne);

      }
      catch(Throwable t)
      {
        if (return_node != null)
        {
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
