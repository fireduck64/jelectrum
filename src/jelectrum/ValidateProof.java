package jelectrum;

import java.net.Socket;
import java.util.Scanner;
import java.io.PrintStream;

import org.json.JSONObject;
import org.json.JSONArray;

import java.util.TreeMap;

public class ValidateProof
{
  public static void main(String args[]) throws Exception
  {
    if (args.length != 3)
    {
      System.out.println("Params: host port address");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String addr = args[2];

    new ValidateProof(host, port, addr);

  }

  public ValidateProof(String host, int port, String addr)
    throws Exception
  {
    Socket sock = new Socket(host, port);

    Scanner scan = new Scanner(sock.getInputStream());

    PrintStream out = new PrintStream(sock.getOutputStream());

    String utxo_root=getUtxoHash(scan, out);
    System.out.println("utxo_root: " + utxo_root);

    TreeMap<String, String> proof_map = getProof(addr, scan, out);
    System.out.println("Proof: " + proof_map);

  }

  private TreeMap<String, String> getProof(String addr, Scanner in, PrintStream out)
    throws Exception
  {
    JSONObject request = new JSONObject();
    request.put("id", "proof");
    JSONArray arr = new JSONArray();
    arr.put(addr);
    request.put("params", arr);
    request.put("method", "blockchain.address.get_proof");

    out.println(request.toString());

    String line = in.nextLine();
    JSONObject reply = new JSONObject(line);

    JSONArray pa = reply.getJSONArray("result");
    TreeMap<String, String> proof_map = new TreeMap<String, String>();
    for(int i=0; i<pa.length(); i++)
    {
      JSONArray s = pa.getJSONArray(i);
      proof_map.put(s.getString(0), s.getString(1));
    }


    return proof_map;

  }

  private String getUtxoHash(Scanner in, PrintStream out)
    throws Exception
  {
    JSONObject o =new JSONObject();
    o.put("id","block");
    o.put("method","blockchain.headers.subscribe");
    out.println(o.toString());

    String line = in.nextLine();
    JSONObject reply = new JSONObject(line);

    return reply.getJSONObject("result").getString("utxo_root");
  }



}
