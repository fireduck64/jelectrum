
import org.junit.Assert;
import org.junit.Test;

import jelectrum.UtxoTrieNode;
import jelectrum.Jelectrum;
import jelectrum.Config;
import jelectrum.UtxoTrieMgr;

import jelectrum.db.DB;

import java.util.Map;
import java.util.HashMap;
import java.util.Scanner;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;

import jelectrum.TXUtil;
import jelectrum.SerializedBlock;
import java.io.PrintStream;
import java.io.File;

public class UtxoTest
{
  @Test
  public void testUtxoNode()
    throws Exception
  {
    UtxoTrieNode a = new UtxoTrieNode("a");

    serialTest(a);

    a.getSprings().put("meow", null);
    serialTest(a);
    a.getSprings().put("notnull", new Sha256Hash("4608c52cd46a96450a48ae518c7f0c3c874024b54a64d7505d85bf86f9b27277"));
    serialTest(a);

    UtxoTrieNode b = new UtxoTrieNode("");
    b.getSprings().put("",null);

    serialTest(b);

  }

  private void serialTest(UtxoTrieNode a)
    throws Exception
  {
    UtxoTrieNode b = new UtxoTrieNode(a.serialize());

    Assert.assertEquals(a.getPrefix(), b.getPrefix());
    System.out.println(a.getSprings());
    System.out.println(b.getSprings());
    Assert.assertEquals(a.getSprings(), b.getSprings());

  }

  @Test
  public void testUtxoTreeAddBlocksNoFlush()
    throws Exception
  {
    testUtxoTreeAddBlocks(false);
  }
  @Test
  public void testUtxoTreeAddBlocksFlush()
    throws Exception
  {
    testUtxoTreeAddBlocks(true);
  }

  private void testUtxoTreeAddBlocks(boolean flush)
    throws Exception
  {
    Jelectrum jelly_test = new Jelectrum(new Config("jelly-memory.conf"));
    Jelectrum jelly_real = new Jelectrum(new Config("jelly-test.conf"));

    UtxoTrieMgr mgr = jelly_test.getUtxoTrieMgr();
    mgr.setTxUtil(new TXUtil(jelly_real.getDB(), jelly_real.getNetworkParameters()));
    mgr.resetEverything();

    Map<Integer, Sha256Hash> hash_check = readCheckFile(1000);

    //String name = "utxo-tree-flush.txt";
    //if (!flush) name = "utxo-tree-noflush.txt";

    //new File(name).delete();

    //PrintStream p_out = new PrintStream(new FileOutputStream(name));

    for(int i=1; i<8192; i++)
    {
      Sha256Hash block_hash = jelly_real.getBlockChainCache().getBlockHashAtHeight(i);

      SerializedBlock sb = jelly_real.getDB().getBlockMap().get(block_hash);
      Block b = sb.getBlock(jelly_real.getNetworkParameters());

      mgr.addBlock(b);

      Sha256Hash expected_hash = hash_check.get(i);
      
      //p_out.println("--------------------------------------");
      //p_out.println("HEIGHT: " + i);
      //mgr.printTree(p_out);
      //mgr.printTree(p_out);
      //mgr.getRootHash();
      //mgr.printTree(p_out);

      if (expected_hash != null)
      {
        Assert.assertEquals("utxo root on block: " + i, expected_hash, mgr.getRootHash());
      }

      if (flush)
      {
        mgr.flush();
      }

      

    }

  }


  private Map<Integer, Sha256Hash> readCheckFile(int n)
    throws Exception
  {
    Scanner scan = new Scanner(new FileInputStream("check/utxo-root-file"));

    Map<Integer, Sha256Hash> m = new HashMap<>();

    while((scan.hasNextLine()) && (m.size() < n))
    {
      int height = scan.nextInt();
      Sha256Hash utxo_hash = new Sha256Hash(scan.next());
      m.put(height, utxo_hash);
    }
    return m;

  }




}
