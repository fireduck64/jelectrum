
package jelectrum;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.StringTokenizer;

public class BlockScan
{
  public static void main(String args[]) throws Exception
  {
    Scanner scan = new Scanner(new FileInputStream("/var/hdd/clash/block-address.txt"));

    TreeMap<String, HashSet<String> > addrmap=new TreeMap<>();
    int lastblock = 0;

    while(scan.hasNextLine())
    {
      String line = scan.nextLine();

      StringTokenizer stok = new StringTokenizer(line, ":");
      int block = Integer.parseInt(stok.nextToken());
      String addr = stok.nextToken();

      addr = addr.substring(0, 10);

     
      if (block != lastblock)
      {
        System.out.println(block);
        lastblock = block;
      }
      
      if ((block >= 470000) && (block < 480000))
      {
        addString("b1-" + block, addr, addrmap);
        int b10 = block - (block % 10);
        int b100 = block - (block % 100);
        int b1000 = block - (block % 1000);
        int b10000 = block - (block % 10000);

        addString("b10-" + b10, addr, addrmap);
        addString("b100-" + b100, addr, addrmap);
        addString("b1000-" + b1000, addr, addrmap);
        addString("b10000-" + b10000, addr, addrmap);
      }

      if (block < 469000) break;
      
    }
    PrintStream report = new PrintStream(new FileOutputStream("block-addr-report.txt"));
    for(String s : addrmap.keySet())
    {
      report.println(s + " - " + addrmap.get(s).size());
    }
  }

  private static void addString(String k, String v, TreeMap<String, HashSet<String> > addrmap)
  {
    HashSet<String> hs = addrmap.get(k);
    if (hs == null)
    {
      hs = new HashSet<String>(16,0.80f);
      addrmap.put(k, hs);
    }
    hs.add(v);
  }

}
