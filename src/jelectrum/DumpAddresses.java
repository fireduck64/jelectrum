
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
import java.util.TreeSet;
import org.bitcoinj.core.LegacyAddress;
import com.google.protobuf.ByteString;
import org.bitcoinj.core.Address;



public class DumpAddresses
{
  public static void main(String args[]) throws Exception
  {
    
    Jelectrum jelly = new Jelectrum(new Config(args[0]));
    int height_start = Integer.parseInt(args[1]);
    int height_end = Integer.parseInt(args[2]);
    String file = args[3];

    new DumpAddresses(jelly, height_start, height_end, file);

  }

  public DumpAddresses(final Jelectrum jelly, int height_start, int height_end, String filename) throws Exception
  {
    ThreadPoolExecutor exec = new ThreadPoolExecutor(16, 16, 2, TimeUnit.DAYS, 
    new LinkedBlockingQueue<Runnable>());
    final TXUtil txutil = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());
    final PrintStream pout = new PrintStream(new FileOutputStream(filename, false));

    for(int j=height_end; j>=height_start; j--)
    {
      final int h = j;

      exec.execute(new Runnable() {

      public void run()
      {
        try
        {

          Sha256Hash blockHash = jelly.getDB().getHeightMap().get(h);
          SerializedBlock sb = jelly.getDB().getBlock(blockHash);
          Block b = sb.getBlock(jelly.getNetworkParameters());
    
          TreeSet<String> addresses = new TreeSet<>();

          for(Transaction tx : b.getTransactions())
          {
            for(ByteString bs : txutil.getAllOutputScriptHashes(tx, true, null))
            {
              String a = getAddressFromScriptHash(jelly, bs);
              addresses.add(a);

            }
          }

          System.err.println("" + h + " addresses: " + addresses.size());
          synchronized(pout)
          {
            for(String addr : addresses)
            {
              pout.println(h +":" + addr);
            }
          }


        }
        catch(Throwable t){t.printStackTrace(); System.exit(-1);}
      }}
      
      
      );

    }

    exec.shutdown();

    

  }

  public String getAddressFromScriptHash(Jelectrum jelly, ByteString hash)
  {

    Address a = LegacyAddress.fromPubKeyHash(jelly.getNetworkParameters(), Util.RIPEMD160(hash).toByteArray());
    return a.toString();
  }


}
