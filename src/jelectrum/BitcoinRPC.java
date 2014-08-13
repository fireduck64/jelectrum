package jelectrum;

import java.net.URL;
import java.util.Scanner;
import java.net.HttpURLConnection;
import java.io.OutputStream;
import org.apache.commons.codec.binary.Base64;

import org.json.JSONObject;
import org.json.JSONArray;
import org.apache.commons.codec.binary.Hex;
import com.google.bitcoin.core.Block;
import java.util.Random;

public class BitcoinRPC
{
  
    private String username;
    private String password;
    private String host;
    private int port;

    public BitcoinRPC(Config config)
    {
        config.require("bitcoind_username");
        config.require("bitcoind_password");
        config.require("bitcoind_host");
        config.require("bitcoind_port");

        username=config.get("bitcoind_username");
        password=config.get("bitcoind_password");
        host=config.get("bitcoind_host");
        port=config.getInt("bitcoind_port");
    }


    private String getUrl()
    {
        return "http://" + host + ":" + port + "/";
    }

    private String getUrlCommand(String cmd)
    {
        return getUrl();
    }

    public JSONObject sendPost(JSONObject post)
        throws java.io.IOException, org.json.JSONException
    {
        //System.out.println(post.toString(2));
        String str = sendPost(getUrl(), post.toString());
        return new JSONObject(str);
    }

    protected String sendPost(String url, String postdata)
        throws java.io.IOException
    {
        URL u = new URL(url);

        HttpURLConnection connection = (HttpURLConnection) u.openConnection(); 

        String basic = username+":"+password;
        String encoded = Base64.encodeBase64String(basic.getBytes()); 
        connection.setRequestProperty("Authorization", "Basic "+encoded);
        connection.setDoOutput(true);
        connection.setDoInput(true);
        connection.setInstanceFollowRedirects(false); 
        connection.setRequestMethod("POST"); 
        connection.setRequestProperty("charset", "utf-8");
        connection.setRequestProperty("Content-Length", "" + Integer.toString(postdata.getBytes().length));
        connection.setUseCaches (false);

        OutputStream wr = connection.getOutputStream ();
        wr.write(postdata.getBytes());
        wr.flush();
        wr.close();

        Scanner scan;

        if (connection.getResponseCode() != 500)
        {
            scan = new Scanner(connection.getInputStream());
        } else {
            scan = new Scanner(connection.getErrorStream());
        }

        StringBuilder sb = new StringBuilder();

        while(scan.hasNextLine())
        {
            String line = scan.nextLine();
            sb.append(line);
            sb.append('\n');
        }


        scan.close();
        return sb.toString();

    }

    public static String getSimplePostRequest(String cmd)
    {
        return "{\"method\":\""+cmd+"\",\"params\":[],\"id\":1}\n";
    }

    public JSONObject doSimplePostRequest(String cmd)
        throws java.io.IOException, org.json.JSONException
    {
        return sendPost(new JSONObject(getSimplePostRequest(cmd)));
    }

    public JSONObject submitTransaction(String transaction_hex)
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","sendrawtransaction");
        JSONArray params = new JSONArray();
        params.put(transaction_hex);
        msg.put("params", params);
        return sendPost(msg);

    }
    public JSONObject getinfo()
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();
        JSONObject msg = new JSONObject();
        msg.put("id", "" + rnd.nextInt());
        msg.put("method","getinfo");
        return sendPost(msg);

    }

    public void testConnection()
        throws java.io.IOException, org.json.JSONException
    {
        try
        {
            System.out.println(getinfo());
        }
        catch(Throwable t)
        {
            System.out.println("bitcoind getinfo failed - check bitcoind config items");
            t.printStackTrace();
        }
    }
 
    public JSONObject submitBlock(Block blk)
        throws java.io.IOException, org.json.JSONException
    {
        Random rnd = new Random();

        JSONObject msg = new JSONObject();
        msg.put("method", "submitblock");
        msg.put("id", "" + rnd.nextInt());
        
        JSONArray params = new JSONArray();
        params.put(Hex.encodeHexString(blk.bitcoinSerialize()));
        msg.put("params", params);
        //System.out.println(msg.toString(2));
        return sendPost(msg);
    }



}
