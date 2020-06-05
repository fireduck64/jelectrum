package elecheck;

import java.io.PrintStream;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
import java.net.Socket;
import java.util.Scanner;
import java.net.URI;
import org.junit.Assert;

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
    //checkBlockHeaderCheckPoint(conn);
    checkBlockHeaders(conn);
    checkBlockchainHeadersSubscribe(conn);
    checkMempoolGetFeeHistogram(conn);

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
    JSONArray params = new JSONArray();
    
    params.add(0);
    params.add(2048);

    JSONObject msg = conn.request("blockchain.block.header", params);
    JSONObject result = (JSONObject) msg.get("result");

    String header = (String) result.get("header");
    
    if (header.length() != 160) throw new Exception("Header not 160 chars"); 
    JSONArray branch = (JSONArray) result.get("branch");

    
    System.out.println(branch.size());

    String root = (String) result.get("root");


    
    
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
