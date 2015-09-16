package jelectrum.db;

import java.util.Random;

import com.google.bitcoin.core.Sha256Hash;

import org.junit.Test;
import com.google.protobuf.ByteString;

public class TestUtil
{

  @Test
  public void testNothing()
  {

  }

  public static Sha256Hash randomHash()
  {
    Random rnd = new Random();
    byte[] b=new byte[32];
    rnd.nextBytes(b);
    return new Sha256Hash(b);
  }
  public static ByteString randomByteString()
  {
    return randomByteString(100);
  }
  public static ByteString randomByteString(int sz)
  {
    Random rnd = new Random();
    byte[] b=new byte[sz];
    rnd.nextBytes(b);

    return ByteString.copyFrom(b);
  }

}
