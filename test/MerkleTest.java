
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;

import jelectrum.Config;
import jelectrum.Jelectrum;
import jelectrum.Util;

import org.json.JSONObject;
import org.json.JSONArray;

import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Block;

public class MerkleTest
{
    private static Jelectrum jelly;

    @BeforeClass
    public static void setup()
        throws Exception
    {
        jelly = new Jelectrum(new Config("jelly-test.conf"));
    }

    @Test
    public void basicMerkle()
        throws Exception
    {
        Sha256Hash block_hash = new Sha256Hash("000000000000000065cbc500ed77a2d8d3891e3b6450b194f1ac5fe8f8da0e62");

        Block blk = jelly.getDB().getBlockMap().get(block_hash).getBlock(jelly.getNetworkParameters());

        JSONObject result = Util.getMerkleTreeForTransaction(blk.getTransactions(), new Sha256Hash("f740f59b18a19edff98bcd5bb83a80b911b70d9378064cd65e5b05229fbe52dc"));

        JSONArray merk = result.getJSONArray("merkle");
        Assert.assertEquals(8, merk.length());

        Assert.assertEquals("61eb064ecab3773efe267a43da6cd12070bd675aa8d8bc2a10040b6c5294d6d4",merk.getString(0));
        Assert.assertEquals("455c597950797d031cbbe593c63837662e200c8fe680c275c392f459caebe6ac",merk.getString(3));

        Assert.assertEquals(34, result.getInt("pos"));

        System.out.println(result);

    }
 
    @Test
    public void bigMerkleFirst()
        throws Exception
    {
        Sha256Hash block_hash = new Sha256Hash("000000000000000083d9b2fcc1d14e53c97604648e4610091ba5f9eb4d1b930b");

        Block blk = jelly.getDB().getBlockMap().get(block_hash).getBlock(jelly.getNetworkParameters());

        JSONObject result = Util.getMerkleTreeForTransaction(blk.getTransactions(), new Sha256Hash("5672d9a9055237c3801ad03af38b75216e23f2f977fe1b38616046d87b6c4c5e"));
        JSONArray merk = result.getJSONArray("merkle");

        Assert.assertEquals(0, result.getInt("pos"));
        Assert.assertEquals(10, merk.length());
        Assert.assertEquals("e958da99798f1edb0455334d1eba8cb924d0192ff5eebd759ae932492e7eb616",merk.getString(5));
        Assert.assertEquals("34325213ee37ae23a5266de8f812de5f13cda4deceb81601783811f858ae201b",merk.getString(9));

        System.out.println(result);

    }
    @Test
    public void bigMerkleLast()
        throws Exception
    {
        Sha256Hash block_hash = new Sha256Hash("000000000000000083d9b2fcc1d14e53c97604648e4610091ba5f9eb4d1b930b");

        Block blk = jelly.getDB().getBlockMap().get(block_hash).getBlock(jelly.getNetworkParameters());

        JSONObject result = Util.getMerkleTreeForTransaction(blk.getTransactions(), new Sha256Hash("32bb3d77f14a69e9998af9e6d60a2b4fa9c2bb0022f271928d01bfe7e4720b69"));
        JSONArray merk = result.getJSONArray("merkle");

        Assert.assertEquals(978, result.getInt("pos"));
        Assert.assertEquals(10, merk.length());
        Assert.assertEquals("6c915f47db641cefdcd379230ad52cb428780a01fe7ce59e25b28607e67d428e",merk.getString(9));

        System.out.println(result);

    }
   

    @Test
    public void testTreeHash()
    {
        Sha256Hash block_hash = new Sha256Hash("0000000021529d056348e04f7ed506286d0b332811e6c0f5f11194d54fccdfff");
        Block blk = jelly.getDB().getBlockMap().get(block_hash).getBlock(jelly.getNetworkParameters());
        Assert.assertEquals(2, blk.getTransactions().size());
        Sha256Hash a = blk.getTransactions().get(0).getHash();
        Sha256Hash b = blk.getTransactions().get(1).getHash();

        Assert.assertEquals(new Sha256Hash("d0e7fce322c043b0f740d4b7ac523f1e58fac3d7ce8782eecbe7e6dc67f68a19"), a);
        Assert.assertEquals(new Sha256Hash("90dfdd37056e892ce908ee2ea864193dc31cd71aa3629bb58c73d657bdf241e8"), b);
        //a = Util.swapEndian(a);
        //b = Util.swapEndian(b);

        //a = Util.doubleHash(a);
        //b = Util.doubleHash(b);


        Sha256Hash tree = Util.treeHash(a,b);

        //tree = Util.swapEndian(tree);

        Sha256Hash expected = blk.getMerkleRoot();

        Assert.assertEquals(expected, tree);
        System.out.println(tree);

    }



}
