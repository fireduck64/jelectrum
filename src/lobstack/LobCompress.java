package lobstack;

import java.util.concurrent.LinkedBlockingQueue;

import java.util.TreeMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.io.File;


public class LobCompress
{

  public static void main(String args[]) throws Exception
  {
    String path = args[0];
    String name = args[1];

    new LobCompress(new File(path), name);

  }

  private Lobstack input;
  private Lobstack output;
  private volatile boolean input_done=false;

  private LinkedBlockingQueue<Map.Entry<String, ByteBuffer> > queue;


  public LobCompress(File path, String name)
    throws Exception
  {
    input = new Lobstack(path, name);

    File com_path = new File(path, "compress");

    com_path.mkdirs();


    output = new Lobstack(com_path, name);


    queue = new LinkedBlockingQueue<Map.Entry<String, ByteBuffer> > (10240);

    new InputThread().start();
    int items = 0;
    long data = 0;

    while((queue.size() > 0) || (!input_done))
    {
      TreeMap<String, ByteBuffer> map = new TreeMap<String, ByteBuffer>();

      while((queue.size() >0) && (map.size() < 1024))
      {
        Map.Entry<String, ByteBuffer> e = queue.take();

        items++;
        data += e.getValue().capacity();

        //System.out.println("Key: " + e.getKey() + " - " + e.getValue().capacity());

        ByteBuffer buf = e.getValue();
        buf.rewind();

        map.put(e.getKey(), buf);
      }
      if (map.size() > 0)
      {
        output.putAll(map);
        System.out.print(".");
      }
      else
      {
        Thread.sleep(100);
      }


    }
    System.out.println();
    System.out.println("Copied " + items + " items, " + data + " bytes");
    output.showSize();

    output.close();

    


  }


  public class InputThread extends Thread
  {
    public void run()
    {
      try
      {
        input.getAll(queue);
        input_done=true;

      }
      catch(Throwable t)
      {
        t.printStackTrace();
        System.exit(-1);
      }

    }

  }

}


