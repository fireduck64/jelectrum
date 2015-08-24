package lobstack;

import java.util.concurrent.LinkedBlockingQueue;

import java.util.TreeMap;
import java.util.Map;
import java.nio.ByteBuffer;
import java.io.File;


public class LobStat
{

  public static void main(String args[]) throws Exception
  {
    String path = args[0];
    String name = args[1];
    

    new LobStat(new File(path), name);

  }

  private Lobstack input;



  public LobStat(File path, String name)
    throws Exception
  {
    input = new Lobstack(path, name);

    input.printTreeStats();   


  }

}

