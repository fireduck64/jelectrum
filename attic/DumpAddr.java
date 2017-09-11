
package jelectrum;

import org.bitcoinj.core.Sha256Hash;

public class DumpAddr
{
  public static void main(String args[]) throws Exception
  {
    Jelectrum jelly = new Jelectrum(new Config(args[0]));

    for(Sha256Hash hash : jelly.getDB().getAddressToTxSet(args[1]))
    {
      System.out.println(hash);
    }

  }

}
