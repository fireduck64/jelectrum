
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.Assert;

import jelectrum.Config;
import jelectrum.Jelectrum;
import jelectrum.ElectrumNotifier;
import jelectrum.Util;
import jelectrum.TXUtil;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.codec.binary.Hex;
import com.google.protobuf.ByteString;

import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.Sha256Hash;

public class ScriptHashTest
{
    private static Jelectrum jelly;
    private static TXUtil tx_util;


    @BeforeClass
    public static void setup()
        throws Exception
    {
        jelly = new Jelectrum(new Config("/home/clash/projects/jelectrum.git/jelly-test.conf"));

        tx_util = jelly.getDB().getTXUtil();
    }


    @Test
    public void testBasicAddresses()
        throws Exception
    {
      testAddressToHash("1CSHyQtcMmezfHP6992T8vgAMbepfGCuer","df7d51b08b541839d528b99e7f07789e5fb81c242d01467ab0fd7b161ef25296");
      testAddressToHash("17azqT8T16coRmWKYFj3UjzJuxiYrYFRBR", "c9aecd1fef8d661a42c560bf75c8163e337099800b8face5ca3d1393a30508a7");
      testAddressToHash("1GPHVTY8UD9my6jyP4tb2TYJwUbDetyNC6", "f5914651408417e1166f725a5829ff9576d0dbf05237055bf13abd2af7f79473");
    }

    @Test
    public void testMultiAddress()
      throws Exception
    {
      testAddressToHash("3D1xj9RM8aD8Fw3B24rJ9errXdJgKcYmEz","e1330673af8936ddfdbcedfb6b67ae3fc3fbc2b005edca31a97c5b80350fd598");
    }
    @Test
    public void testTransactionNorm()
      throws Exception
    {
      testTxOut("9a728158a2aad7f096ba47615af57ebf6d875abd3352180bcfb2c0503bcb4b0d",1,"bf6c8a6f153352f1af46320ff9bac1db94cac868ce2353b4dd98e9d76339ad57");
      testTxIn("9a728158a2aad7f096ba47615af57ebf6d875abd3352180bcfb2c0503bcb4b0d",0,"bf6c8a6f153352f1af46320ff9bac1db94cac868ce2353b4dd98e9d76339ad57");

    }

    @Test
    public void testBechAddress()
      throws Exception
    {
      // Bitcoinj can't parse these yet but shouldn't be an issue, anyone new enough to be using
      // these will most likely be using scripthash rather than addresses anyways
      //testAddressToHash("bc1qwqdg6squsna38e46795at95yu9atm8azzmyvckulcc7kytlcckxswvvzej", "43f626c7e6c22741e3160900776ba65f2526956c54779d193267fde55624adaf");
    }



    @Test
    public void testTransactionMulti()
      throws Exception
    {
      testTxOut("85057000b0892d25639016603849c8c6edd894e04b09e6c8138e71217c250e0d",1,"e1330673af8936ddfdbcedfb6b67ae3fc3fbc2b005edca31a97c5b80350fd598"); 
      testTxIn("f5a4fedccdfedbda43cf05c694f1626cd785d7b3d8408b9221f89370eb43b6e5", 0, "e1330673af8936ddfdbcedfb6b67ae3fc3fbc2b005edca31a97c5b80350fd598");
    }

    @Test
    public void testSegwit()
      throws Exception
    {
      testTxIn("fc932bedf2acde8ed9daa31ecb9645ac1cae2da6f34f1e5da510e31d40bcfea8", 0, "9323dfc40927f75291054f82f2204770bc1a2e670d621dbcd0ffaa64b98824de");
      testTxOut("fc932bedf2acde8ed9daa31ecb9645ac1cae2da6f34f1e5da510e31d40bcfea8", 0,"84e4a2db6254695b80c49bdc12727b3ba05a972a6786aec3635e3919c3265d6f");
      testTxOut("fc932bedf2acde8ed9daa31ecb9645ac1cae2da6f34f1e5da510e31d40bcfea8", 1,"0295e6e747044d50ce6a6ddb7eb86723722f4aa3342ee9b4a54bc490b13cccf6");
    }

    @Test
    public void testBech()
      throws Exception
    {
      testTxIn("0317c467eaf898a751b5ff597176a96f2b002057ffc28828c4c740684cce8b78",0,"43f626c7e6c22741e3160900776ba65f2526956c54779d193267fde55624adaf");
      testTxOut("0317c467eaf898a751b5ff597176a96f2b002057ffc28828c4c740684cce8b78",0,"4a7b3ea6a7307ac77e920f514dc4a5d2ea7825722c29102488621bf27a2f23c9");
      testTxOut("0317c467eaf898a751b5ff597176a96f2b002057ffc28828c4c740684cce8b78",1,"43f626c7e6c22741e3160900776ba65f2526956c54779d193267fde55624adaf");
    }


    private void testTxOut(String tx_hash, int output, String scripthash_str)
      throws Exception
    {
      Transaction tx = jelly.getDB().getTransaction(Sha256Hash.wrap(tx_hash)).getTx(jelly.getNetworkParameters());

      ByteString scripthash = tx_util.getScriptHashForOutput(tx.getOutputs().get(output));

      Assert.assertEquals(scripthash_str, getHexString(scripthash));

    }

    private void testTxIn(String tx_hash, int in, String scripthash_str)
      throws Exception
    {
      Transaction tx = jelly.getDB().getTransaction(Sha256Hash.wrap(tx_hash)).getTx(jelly.getNetworkParameters());

      ByteString scripthash = tx_util.getScriptHashForInput(tx.getInputs().get(in), true, null);

      Assert.assertEquals(scripthash_str, getHexString(scripthash));

    }




    private void testAddressToHash(String addr, String scripthash_str)
    {
      ByteString scripthash = tx_util.getScriptHashForAddress(addr);

      Assert.assertEquals(scripthash_str, getHexString(scripthash));

    }


    private String getHexString(ByteString bs)
    {
      return new String(Hex.encodeHex(bs.toByteArray()));
    }



}
