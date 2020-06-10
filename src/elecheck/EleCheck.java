package elecheck;

import java.io.PrintStream;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
import java.net.Socket;
import java.util.Scanner;
import java.net.URI;
import org.junit.Assert;
import org.bitcoinj.core.Sha256Hash;
import jelectrum.Util;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import snowblossom.lib.HexUtil;
import java.util.Random;

/**
 * General test of an electrum server.
 * Only worried about protocol 1.4+ features
 * On problems, only reports first error and does not continue
 */

public class EleCheck
{
  public static void main(String args[]) throws Exception
  {
    String url = args[0]; 

    check(url, System.out);

  }

  public static void check(String url, PrintStream out)
    throws Exception
  {
    out.println("Checking: " + url);
    URI uri = new URI(url);

    String host = uri.getHost();
    int port = uri.getPort();

    Socket sock = new Socket(host, port);

    EleConn conn = new EleConn(sock);
   
    checkServerVersion(conn);
    //checkBlockHeader(conn);
    //checkBlockHeaders(conn);
    checkBlockchainHeadersSubscribe(conn);
    checkMempoolGetFeeHistogram(conn);
    checkBlockHeaderCheckPoint(conn);


  }

  public static void checkServerVersion(EleConn conn)
  {
  
    JSONArray params = new JSONArray();
    params.add("EleCheck");

    JSONArray range = new JSONArray();
    range.add("0.10");
    range.add("1.4.2");

    params.add(range);

    JSONObject msg = conn.request("server.version", params);
    JSONArray result = (JSONArray) msg.get("result");

    String server_id = (String)result.get(0);
    String ver = (String)result.get(1);

    System.out.println("Server id: " + server_id);
    System.out.println("Server selected version: " + ver);

  }
  
  public static void checkBlockHeader(EleConn conn)
    throws Exception
  {
    JSONArray params = new JSONArray();
    
    params.add(100000);

    JSONObject msg = conn.request("blockchain.block.header", params);
    String header = (String) msg.get("result");

    if (header.length() != 160) throw new Exception("Header not 160 chars"); 

    
    
  }

  public static void checkBlockHeaderCheckPoint(EleConn conn)
    throws Exception
  {
    
    //int blk_n = 109;
    //int cp = 32911;
    Random rnd = new Random();

    for(int i=0; i<20; i++)
    {
      JSONArray params = new JSONArray();
      int cp = rnd.nextInt(630000)+10;
      int blk_n = rnd.nextInt(cp);
      //cp = 5;
      //blk_n = 2;

      params.add(blk_n);
      params.add(cp);

      JSONObject msg = conn.request("blockchain.block.header", params);
      JSONObject result = (JSONObject) msg.get("result");

      String header = (String) result.get("header");
      
      if (header.length() != 160) throw new Exception("Header not 160 chars"); 
      JSONArray branch = (JSONArray) result.get("branch");

      String header_hex = (String) result.get("header");

      
      String root = (String) result.get("root");
      validateMerkle(HexUtil.hexStringToBytes(header_hex), branch, blk_n, cp, root);

    }
    

    
  }

  protected static void validateMerkle(ByteString header, JSONArray branch, int target, int checkpoint, String expected_root)
  {
    int loc = target;

    Sha256Hash cur_hash = Util.swapEndian(Util.hashDoubleBs(header));

    int level = 0;
    while(checkpoint > 0)
    {
      Sha256Hash other = Sha256Hash.wrap((String) branch.get(level));
      if (level ==0)
      {
        System.out.println(other+ " " + cur_hash);
      }

      if (loc % 2 == 0) // we are left
      {
        cur_hash = Util.treeHash(cur_hash, other);  
      }
      else
      { // we are right
        cur_hash = Util.treeHash(other, cur_hash);
      }
      loc /= 2;
      level++;
      checkpoint /= 2;

    }

    Sha256Hash root = cur_hash;
    System.out.println("root: " + root);

    Assert.assertEquals(root.toString(), expected_root);


  }

  public static void checkBlockHeaders(EleConn conn)
    throws Exception
  {
    JSONArray params = new JSONArray();
    
    params.add(100000);
    params.add(10);

    JSONObject msg = conn.request("blockchain.block.headers", params);
    JSONObject result = (JSONObject) msg.get("result");
    
  }


  public static void checkBlockchainHeadersSubscribe(EleConn conn)
    throws Exception
  {
    JSONArray params = new JSONArray();
    
    JSONObject msg = conn.request("blockchain.headers.subscribe");
    JSONObject result = (JSONObject) msg.get("result");

    int height = (int)result.get("height");
    String hex = (String)result.get("hex");

    Assert.assertEquals(160, hex.length());
  }

  public static void checkMempoolGetFeeHistogram(EleConn conn)
    throws Exception
  {
    JSONArray params = new JSONArray();
    
    JSONObject msg = conn.request("mempool.get_fee_histogram");
    JSONArray result = (JSONArray) msg.get("result");

    if (result.size() == 0)
    {
      System.out.println("Warning: empty mempool or mempool.get_fee_histogram broken");
    }

  }

}
