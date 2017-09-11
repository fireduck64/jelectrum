
package jelectrum;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.bitcoinj.core.StoredBlock;
import org.bitcoinj.core.Block;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import org.apache.commons.codec.binary.Hex;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;




public class DumpBlock
{
  public static void main(String args[]) throws Exception
  {
    
    Jelectrum jelly = new Jelectrum(new Config(args[0]));
    int height_start = Integer.parseInt(args[1]);
    int height_end = Integer.parseInt(args[2]);

    new DumpBlock(jelly, height_start, height_end);

  }

  public DumpBlock(final Jelectrum jelly, int height_start, int height_end) throws Exception
  {
    ThreadPoolExecutor exec = new ThreadPoolExecutor(16, 16, 2, TimeUnit.DAYS, 
    new LinkedBlockingQueue<Runnable>());

    for(int j=height_end; j>=height_start; j--)
    {
      final int h = j;

      exec.execute(new Runnable() {

      public void run()
      {
      try
      {

      Sha256Hash blockHash = jelly.getDB().getHeightMap().get(h);
      SerializedBlock sb = jelly.getDB().getBlockMap().get(blockHash);
      Block b = sb.getBlock(jelly.getNetworkParameters());
  
      int count = 0;
      int strange = 0;
      for(Transaction tx : b.getTransactions())
      {
        /*if (DumpTx.hasStrangeData(tx))
        {
          System.out.println("TX is strange " + tx.getHash());
          strange++;
          String dir_name = "" + h;
          while(dir_name.length() < 6) dir_name = "0" + dir_name;
          File dir = new File("/var/ssd/clash/strange/" + dir_name);
          dir.mkdirs();

          byte[] serial = tx.bitcoinSerialize();
          FileOutputStream fout = new FileOutputStream(new File(dir, tx.getHash() + ".txt"), false);
          PrintStream pout = new PrintStream(fout);

          String hex = Hex.encodeHexString(serial);

          pout.println(hex);

          pout.flush();
          pout.close();
        }*/
        String match = DumpTx.hasHeaderData(tx);
        if (match!=null)
        {
          if (DumpTx.hasStrangeData(tx)) match = "big_" + match;
          System.out.println("TX has header " + tx.getHash());
          strange++;
          String dir_name = "" + h;
          while(dir_name.length() < 6) dir_name = "0" + dir_name;
          File dir = new File("/var/ssd/clash/scan/" + dir_name);
          dir.mkdirs();

          byte[] serial = tx.bitcoinSerialize();
          FileOutputStream fout = new FileOutputStream(new File(dir, match +"." +tx.getHash()+ ".txt"), false);
          FileOutputStream bout = new FileOutputStream(new File(dir, match +"." +tx.getHash()+ ".bin"), false);

          bout.write(serial);
          bout.flush();
          bout.close();
          PrintStream pout = new PrintStream(fout);

          String hex = Hex.encodeHexString(serial);

          pout.println(hex);

          pout.flush();
          pout.close();
        }
        count++;
      }

      System.err.println("" + h + " checked: " + count + " Strange: " + strange);

          }
          catch(Throwable t){t.printStackTrace(); System.exit(-1);}
      }}
      
      
      );

    }

    exec.shutdown();

    

  }


}
