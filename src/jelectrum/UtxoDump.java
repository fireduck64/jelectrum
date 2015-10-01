package jelectrum;

import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;
import java.util.zip.GZIPOutputStream;
import java.io.FileOutputStream;

public class UtxoDump
{
  public static void main(String args[]) throws Exception
  {
    if (args.length != 3)
    {
      System.out.println("Syntax: config (load|dump) file");
      return;
    }

    Config config = new Config(args[0]);

    String direction=args[1];
    String file_name = args[2];

    Jelectrum jelly = new Jelectrum(config);

    if (direction.equals("dump"))
    {
      FileOutputStream f_out = new FileOutputStream(file_name);
      GZIPOutputStream z_out = new GZIPOutputStream(f_out);
      jelly.getUtxoTrieMgr().dumpDB(z_out);

      z_out.close();
    }
    else if (direction.equals("load"))
    {
      FileInputStream f_in = new FileInputStream(file_name);
      GZIPInputStream z_in = new GZIPInputStream(f_in);
      jelly.getUtxoTrieMgr().loadDB(z_in);
      z_in.close();
    }
    else
    {
      System.out.println("Direction: " + direction + " makes no sense");
    }


  }

}
