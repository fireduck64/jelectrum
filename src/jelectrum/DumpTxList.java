package jelectrum;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptChunk;
import org.bitcoinj.core.Address;
import org.apache.commons.codec.binary.Hex;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import java.util.Scanner;
import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;


public class DumpTxList
{
  public static void main(String args[]) throws Exception
  {
    Jelectrum jelly = new Jelectrum(new Config(args[0]));

    Scanner scan = new Scanner(new FileInputStream(args[1]));

    PrintStream pout = new PrintStream(new FileOutputStream(args[2], false));

    TXUtil txutil = new TXUtil(jelly.getDB(), jelly.getNetworkParameters());

    while(scan.hasNext())
    {
      String hash = scan.next();
      Transaction tx = jelly.getDB().getTransaction(new Sha256Hash(hash)).getTx(jelly.getNetworkParameters());


      int in_idx =0;
      for(TransactionInput in : tx.getInputs())
      {
        Address addr = in.getFromAddress();

        byte[] h160 = addr.getHash160();

        pout.println("txin:" + hash + ":" + in_idx + ":" + Hex.encodeHexString(h160));

        in_idx++;

        /*System.out.println("Input: " + in);
        Script script = in.getScriptSig();
        for(ScriptChunk chunk : script.getChunks())
        {
          if (chunk.isOpCode())
          {
            System.out.println("    op " + chunk.opcode);
          }
          if (chunk.isPushData() && (chunk.data != null))
          {
            System.out.println("    data " + chunk.data.length);
          }

        }*/
      }
      pout.println("tx:" + hash + ":" + txutil.getTXBlockHeight(tx, jelly.getBlockChainCache()));

    for(TransactionOutput out : tx.getOutputs())
    {
      int idx = out.getIndex();
      Script script = out.getScriptPubKey();

      for(ScriptChunk chunk : script.getChunks())
      {
        if (chunk.isOpCode())
        {
          //System.out.println("    op " + chunk.opcode);
        }
        if (chunk.isPushData() && (chunk.data != null))
        {
          pout.println("txout:" + hash + ":" + idx + ":" + Hex.encodeHexString(chunk.data));
        }

      }


    }

    }

    pout.flush();
    pout.close();



  }

  private static TreeSet<String> funHeader;

  private synchronized static void buildHeaders()
  {
    if (funHeader != null) return;

   


    TreeSet<String> funHeaders = new TreeSet<String>();
    funHeaders.add("377abcaf271c"); //7z
    funHeaders.add("1c27afbc7a37");

    ArrayList<String> bits = new ArrayList<String>();
    Map<String, Integer> revMap = new TreeMap<String, Integer>();
    for(int i=0; i<256; i++)
    {
      String b = String.format("%x", i);
      if (b.length() < 2) b="0" + b;
      bits.add(b);
      revMap.put(b, i);
    }
    //System.out.println(bits);
    //System.out.println(revMap);

    TreeSet<String> newHeaders = new TreeSet<>();

    for(int i=1; i<256; i++)
    {
      for(String s : funHeaders)
      {
        StringBuilder sb = new StringBuilder();
        for(int j=0; j<s.length(); j+=2)
        {
          String sub = s.substring(j,j+2);

          int off = revMap.get(sub);
          int newoff = (off + i) % 256;

          sb.append(bits.get(newoff));
        }

        newHeaders.add(sb.toString());
      }

    }
    //System.out.println(funHeaders);
    //System.out.println(newHeaders);

    funHeaders.addAll(newHeaders);
    //System.out.println(funHeaders);

    funHeaders.add("d0cf11e0a1b11ae1");
    funHeaders.add("576f72642e446f63756d656e742e");
    funHeaders.add("d0cf11e0a1b11ae1");
    funHeaders.add("feffffff000000000000000057006f0072006b0062006f006f006b00");
    funHeaders.add("d0cf11e0a1b11ae1");
    funHeaders.add("a0461df0");
    funHeaders.add("504b030414");
    funHeaders.add("504b050600");
    funHeaders.add("504b030414000100630000000000");
    funHeaders.add("ffd8ffe000104a464946000101");
    funHeaders.add("474946383961");
    funHeaders.add("474946383761");
    funHeaders.add("2100003b00");
    funHeaders.add("25504446");
    funHeaders.add("2623323035");
    funHeaders.add("2525454f46");
    funHeaders.add("616e6e6f756e6365");
    funHeaders.add("1f8b08");
    funHeaders.add("504b03040a000200");
    funHeaders.add("89504e470d0a1a0a");
    funHeaders.add("6d51514e42");
    funHeaders.add("6d51494e4246672f");
    funHeaders.add("6d51454e424667");
    funHeaders.add("526172211a0700");
    funHeaders.add("efedface");
    funHeaders.add("4f676753");
    funHeaders.add("4d546864");
    funHeaders.add("377abcaf271c");
    funHeaders.add("4a756c69616e20417373616e6765");


    funHeader = funHeaders;
    
  }

  public static String hasHeaderData(Transaction tx)
  {
    if (funHeader == null) buildHeaders();

    byte[] serial = tx.bitcoinSerialize();
    String hex = Hex.encodeHexString(serial);

    for(String c : funHeader)
    {
      if (hex.contains(c)) return c;
    }

    return null;

  }

  public static boolean hasStrangeData(Transaction tx)
  {
    try
    {
    boolean hasStrange = false;
    /*for(TransactionInput in : tx.getInputs())
    {
      Script script = in.getScriptSig();
      int data_in = 0;
       for(ScriptChunk chunk : script.getChunks())
      {
        if (chunk.isOpCode())
        {
        }
        if (chunk.isPushData() && (chunk.data != null))
        {
          data_in += chunk.data.length;
        }

      }
      if (data_in > 20) return true;

    }*/
    int extra_data = 0;

    for(TransactionOutput out : tx.getOutputs())
    {
      int data_out = 0;
      Script script = out.getScriptPubKey();

      for(ScriptChunk chunk : script.getChunks())
      {
        if (chunk.isOpCode())
        {
        }
        if (chunk.isPushData() && (chunk.data != null))
        {
          data_out += chunk.data.length;
        }

      }
      if (data_out > 20) extra_data += data_out;


    }

    if (extra_data > 20) return true;

    return false;
    }

    catch(Throwable t){return true;}


  }

}
