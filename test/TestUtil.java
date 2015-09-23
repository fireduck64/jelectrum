
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
    Random rnd = new Random();
    byte[] b=new byte[100];
    rnd.nextBytes(b);

    return ByteString.copyFrom(b);
  }

  public static byte[] randomBytes(int len)
  {
    Random rnd = new Random();
    byte[] b= new byte[len];
    rnd.nextBytes(b);

    return b;
  }

}
