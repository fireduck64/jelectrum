
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;

import jelectrum.Config;
import jelectrum.Jelectrum;
import jelectrum.TXUtil;
import jelectrum.ElectrumNotifier;
import jelectrum.Util;
import java.util.Collection;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;

import org.json.JSONObject;
import org.json.JSONArray;


public class AddressTest
{
    private static Jelectrum jelly;
    private static TXUtil tx_util;

    @BeforeClass
    public static void setup()
        throws Exception
    {
        jelly = new Jelectrum(new Config("jelly-test.conf"));
        tx_util = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());
    }

    @Test
    public void testOldCoinbase()
    {
        Sha256Hash tx_hash = new Sha256Hash("bcdc61cbecf6137eec5c8ad4047fcdc36710e77e404b17378a33ae605920afe1");

        Transaction tx = tx_util.getTransaction(tx_hash);


        Collection<String> lst = tx_util.getAllAddresses(tx, true, null);

        Assert.assertEquals(1, lst.size());
        Assert.assertEquals("13PHR5QM2cJLkFoA6E3rPEwTyYxxSCJ3B4", lst.iterator().next());

    }
    @Test
    public void testSpendOldCoinbase()
        throws Exception
    {
        Sha256Hash tx_hash = new Sha256Hash("5e86b6609207e3376ebddde5e96da2b33ccfba3783f2389cb2aaad6452be985d");

        Transaction tx = tx_util.getTransaction(tx_hash);

        Assert.assertNotNull(tx);

        Collection<String> lst = tx_util.getAllAddresses(tx, true, null);

        Assert.assertEquals(2, lst.size());
        Assert.assertEquals("13PHR5QM2cJLkFoA6E3rPEwTyYxxSCJ3B4", lst.iterator().next());

    }

 
    @Test
    public void testModernCoinbase()
    {
        Sha256Hash tx_hash = new Sha256Hash("21abdaab7f5062f205c0a11e9476ec6dbdc3cca0e40df5951ed8e959839d43c5");

        Transaction tx = tx_util.getTransaction(tx_hash);

        Collection<String> lst = tx_util.getAllAddresses(tx, true, null);

        Assert.assertEquals(1, lst.size());
    }


}
