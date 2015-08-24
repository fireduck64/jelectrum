package lobstack;

import java.util.concurrent.LinkedBlockingQueue;

import java.util.TreeMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.io.File;


public class LobCleanup
{

  public static void main(String args[]) throws Exception
  {
    String path = args[0];
    String name = args[1];
    boolean comp = false;
    if (args.length > 2)
    {
      if (args[2].equals("true"))
      {
        comp=true;
      }
    }

    

    new LobCleanup(new File(path), name, comp);

  }

  private Lobstack input;



  public LobCleanup(File path, String name, boolean comp)
    throws Exception
  {
    input = new Lobstack(path, name, comp);

    input.cleanup(0.25,50*1024*1024);
    input.printTreeStats();   
    //input.printTreeStats();   


  }

}

