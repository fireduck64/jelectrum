package lobstack;

import java.text.DecimalFormat;


public class TreeStat
{
  public long node_count;
  public long data_count;
  public long node_size;
  public long data_size;
  public long node_children;
  public long node_children_max=0;
  public long node_children_min=10000;


  public void print()
  {
    DecimalFormat df = new DecimalFormat("0.000");

    System.out.println("Nodes: " + node_count);
    System.out.println("Data entires: " + data_count);

    double node_gb = node_size / 1024.0 / 1024.0 / 1024.0;
    System.out.println("Node size: " + df.format(node_gb) + " gb");
    double data_gb = data_size  / 1024.0 / 1024.0 / 1024.0;
    System.out.println("Data size: " + df.format(data_gb) + " gb");

    double avg_children = node_children / node_count;
    System.out.println("Children per node: " + node_children_min + " " + df.format(avg_children) + " " + node_children_max);



  }

}
