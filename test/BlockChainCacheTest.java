
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;

import jelectrum.Config;
import jelectrum.Jelectrum;
import jelectrum.BlockChainCache;

import org.json.JSONObject;
import org.json.JSONArray;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.StoredBlock;

public class BlockChainCacheTest
{
    private static Jelectrum jelly;

    @BeforeClass
    public static void setup()
        throws Exception
    {
        jelly = new Jelectrum(new Config("jelly-test.conf"));
    }

    @Test
    public void basicChain()
        throws Exception
    {
        StoredBlock head = jelly.getDB().getBlockStoreMap().get(new Sha256Hash("00000000000ace2adaabf1baf9dc0ec54434db11e9fd63c1819d8d77df40afda"));
        jelly.getBlockChainCache().update(jelly, head);


        Sha256Hash block_750 = new Sha256Hash("00000000ad8174a71c1b2c01fd6076143c2cf57d768bf80d7c11b6721d3a2525");

        Sha256Hash found = jelly.getBlockChainCache().getBlockHashAtHeight(750);
        Assert.assertEquals(block_750, found);

        Assert.assertTrue(jelly.getBlockChainCache().isBlockInMainChain(block_750));

        byte[] data = head.getHeader().bitcoinSerialize();
        Assert.assertEquals(80, data.length);
 

    }



}
