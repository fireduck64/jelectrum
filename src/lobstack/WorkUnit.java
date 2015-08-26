package lobstack;

import java.util.Map;
import java.util.TreeMap;
import jelectrum.SimpleFuture;
import java.nio.ByteBuffer;
import org.junit.Assert;


public class WorkUnit
{
  public WorkUnit(Lobstack stack, NodeEntry ne, TreeMap<Long, ByteBuffer> save_entries)
  {
    this.stack = stack;
    this.ne = ne;
    this.save_entries = save_entries;
  }
  public WorkUnit(Lobstack stack, String prefix, TreeMap<Long, ByteBuffer> save_entries)
  {
    this.stack = stack;
    this.ne = new NodeEntry();
    this.node = new LobstackNode(prefix);
    this.ne.node = true;
    this.save_entries = save_entries;
  }


  public Lobstack stack;
  public LobstackNode node;
  public TreeMap<Long, ByteBuffer> save_entries;
  public Map<String, NodeEntry> put_map=new TreeMap<String, NodeEntry>();
  public SimpleFuture<NodeEntry> return_entry=new SimpleFuture<NodeEntry>();
  public NodeEntry ne;


  public void assertConsistent()
  {
    if (!ne.node) Assert.assertEquals("Non node, put map should be empty",0,put_map.size());
    if (node==null) Assert.assertTrue("Don't have a node, we must have a loaction", ne.location >= 0);
    for(NodeEntry ne : put_map.values())
    {
      if (ne.node)
      {
        Assert.assertTrue("Only existing nodes added to a node", ne.location>=0);
      }
      //Assert.assertFalse("Should not add nodes to node", ne.node);
    }

  }
  
}
