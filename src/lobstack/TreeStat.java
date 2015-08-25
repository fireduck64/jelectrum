package lobstack;
import java.util.TreeMap;

import java.text.DecimalFormat;

import java.io.PrintStream;

public class TreeStat
{
  public long node_count;
  public long data_count;
  public long node_size;
  public long data_size;
  public long node_children;
  public long node_children_max=0;
  public long node_children_min=10000;
  public TreeMap<Integer, Long> file_use_map=new TreeMap<Integer, Long>();

  public void print()
  {
    print(System.out);
  }

  public void print(PrintStream out)
  {
    DecimalFormat df = new DecimalFormat("0.000");

    System.out.println("Nodes: " + node_count);
    System.out.println("Data entires: " + data_count);

    double node_gb = node_size / 1024.0 / 1024.0 / 1024.0;
    out.println("Node size: " + df.format(node_gb) + " gb");
    double data_gb = data_size  / 1024.0 / 1024.0 / 1024.0;
    out.println("Data size: " + df.format(data_gb) + " gb");

    if (node_count > 0)
    {
      double avg_children = node_children / node_count;
      out.println("Children per node: " + node_children_min + " " + df.format(avg_children) + " " + node_children_max);
    }
    System.out.println("File use: ");
    for(int file : file_use_map.keySet())
    {
      String f = "" + file;
      while(f.length() < 4) f="0" + f;
      double perc = file_use_map.get(file) * 1.0 / Lobstack.SEGMENT_FILE_SIZE;
      out.println("  " + f + ": " + df.format(perc));
    }

  }

  public synchronized void addFileUse(long location, long size)
  {
    int file = (int)(location / Lobstack.SEGMENT_FILE_SIZE);
    if (!file_use_map.containsKey(file))
    {
      file_use_map.put(file, 0L);
    }
    file_use_map.put(file, file_use_map.get(file) + size);
  } 

}
