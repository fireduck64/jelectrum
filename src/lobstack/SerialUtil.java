package lobstack;

import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class SerialUtil
{

  public static void writeString(DataOutputStream out, String str)
    throws IOException
  {
    byte[] b= str.getBytes();
    out.writeInt(b.length);
    out.write(b);
  }
  public static String readString(DataInputStream in)
    throws IOException
  {
    int len = in.readInt();
    byte[] b = new byte[len];
    in.readFully(b);
    return new String(b);

  }



}
