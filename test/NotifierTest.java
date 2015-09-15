
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;

import jelectrum.Config;
import jelectrum.Jelectrum;
import jelectrum.ElectrumNotifier;
import jelectrum.Util;

import org.json.JSONObject;
import org.json.JSONArray;


public class NotifierTest
{
    private static Jelectrum jelly;

    @BeforeClass
    public static void setup()
        throws Exception
    {
        jelly = new Jelectrum(new Config("jelly.conf"));
    }


    @Test
    public void testAddressHashNull()
        throws Exception
    {

        Assert.assertNull(jelly.getElectrumNotifier().getAddressChecksum("1rUN1uarnD7BnjXrzuPufT9fnpKpzqQfD"));

        testAddress("1rUN1uarnD7BnjXrzuPufT9fnpKpzqQfD");

    }
    @Test
    public void testAddressHashNormal()
        throws org.json.JSONException
    {
        Assert.assertEquals("c439e52e607b352381b646ebbcdca37f2aedf8bee5f4e9d63b5a7f0ca7d9a442",jelly.getElectrumNotifier().getAddressChecksum("14h89KekfUn2n4dug6GxXJg177CV2m7Gz8"));

        testAddress("14h89KekfUn2n4dug6GxXJg177CV2m7Gz8");

    }

    @Test
    public void testAddressHashCoinbase()
        throws org.json.JSONException
    {

        Assert.assertEquals("7b8716f234c243861e85f73b5bf22cde74ec6b5e28c44c91967a91949496a348",jelly.getElectrumNotifier().getAddressChecksum("13PHR5QM2cJLkFoA6E3rPEwTyYxxSCJ3B4"));

        testAddress("13PHR5QM2cJLkFoA6E3rPEwTyYxxSCJ3B4");

        
    }
    @Test
    public void testAddressHashBaseline()
        throws org.json.JSONException
    {

        Assert.assertEquals("950f8571564e04e504285a41e4bdc198f72418c201c6ce84c9650798d989d700",jelly.getElectrumNotifier().getAddressChecksum("17pFAsFzB1W3v8A1TAR6dnux7frLZEyHJ3"));

        testAddress("17pFAsFzB1W3v8A1TAR6dnux7frLZEyHJ3");

        
    }


    private void testAddress(String address)
        throws org.json.JSONException
    {
        String hash = jelly.getElectrumNotifier().getAddressChecksum(address);

        Object history = jelly.getElectrumNotifier().getAddressHistory(address);
        JSONArray arr = (JSONArray)history;

        if (hash==null)
        {

            Assert.assertEquals(0, arr.length());
            return;
        }

        StringBuilder sb = new StringBuilder();

        for(int i=0; i<arr.length() ; i++)
        {
            JSONObject o = (JSONObject)arr.get(i);

            String tx_hash = o.getString("tx_hash");
            int height = o.getInt("height");
            sb.append(tx_hash);
            sb.append(':');
            sb.append(height);
            sb.append(':');

        }

        String calc_hash = Util.SHA256(sb.toString());
        Assert.assertEquals(calc_hash, hash);



    }



}
