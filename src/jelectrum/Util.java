package jelectrum;

import java.security.MessageDigest;


import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.params.TestNet3Params;

import java.util.Collection;
import java.util.ArrayList;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map;
import java.net.URL;

public class Util
{
  public static final long FEE_MAP_UPDATE_TIME=300000L; //5min

    public static String getHexString(byte[] data)
    {
        StringBuilder sb = new StringBuilder();
        for(byte b : data)
        { 
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }


  public static String SHA256(byte[] P)
  {   
        try
        {   
            MessageDigest SIG=MessageDigest.getInstance("SHA-256");
            SIG.update(P, 0, P.length);

            byte D[]=SIG.digest();

            return getHexString(D);


        }
        catch (java.security.NoSuchAlgorithmException e)
        {   
            throw new RuntimeException(e);


        }

  }

  public static String SHA256(String s)
  {
    return SHA256(s.getBytes());
  }

  public static int measureSerialization(Object obj)
  {
    try
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream obj_out = new ObjectOutputStream(out);

        obj_out.writeObject(obj);
        obj_out.flush();
        obj_out.close();

        return out.size();

    }
    catch(Exception e)
    {
        throw new RuntimeException(e);
    }



  }


    public static Sha256Hash treeHash(Sha256Hash a, Sha256Hash b)
    {
        try
        {

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(swapEndian(a).getBytes());
            md.update(swapEndian(b).getBytes());

            byte[] pass = md.digest();
            md = MessageDigest.getInstance("SHA-256");
            md.update(pass);

            return swapEndian(new Sha256Hash(md.digest()));
        }
        catch(java.security.NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

    }
    public static Sha256Hash doubleHash(Sha256Hash a)
    {
        try
        {

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(a.getBytes());

            byte[] pass = md.digest();
            md = MessageDigest.getInstance("SHA-256");
            md.update(pass);

            return new Sha256Hash(md.digest());
        }
        catch(java.security.NoSuchAlgorithmException e)
        {
            throw new RuntimeException(e);
        }

    }

    public static Sha256Hash swapEndian(Sha256Hash a)
    {
        return new Sha256Hash(swapEndianHexString(a.toString()));
        
    }
    public static String swapEndianHexString(String in)
    {
        StringBuilder sb=new StringBuilder();

        for(int i=0; i<in.length(); i+=2)
        {
            String s = in.substring(i,i+2);
            sb.insert(0,s);
        }
        return sb.toString();
    }




    public static JSONObject getMerkleTreeForTransaction(Collection<Transaction> tx_list, Sha256Hash tx_hash)
    {
        try
        {
            ArrayList<Sha256Hash> tx_hash_list=new ArrayList<Sha256Hash>();
            int target_pos=-1;

            for(Transaction tx : tx_list)
            {
                if (tx.getHash().equals(tx_hash)) target_pos=tx_hash_list.size();
                tx_hash_list.add(tx.getHash());
            }
            if (target_pos < 0) throw new RuntimeException("Target transaction not in collection");

            JSONObject result = new JSONObject();
            result.put("pos", target_pos);

            ArrayList<Sha256Hash> out_list = new ArrayList<Sha256Hash>();
            int span=1;
            while(span < tx_hash_list.size()) span = span*2;
            Sha256Hash root = getInternalMerkleTreeMadness(tx_hash_list, target_pos, out_list);

            //result.put("root", root);

            JSONArray merkle = new JSONArray();
            for(Sha256Hash h : out_list)
            {
                merkle.put(h.toString());
            }
            result.put("merkle", merkle);





            return result;
        }
        catch(org.json.JSONException e)
        {
            throw new RuntimeException(e);
        }



    }

    private static Sha256Hash getInternalMerkleTreeMadness(ArrayList<Sha256Hash> tx_list, int target_pos, ArrayList<Sha256Hash> out_list)
    {
        int magic_pos = target_pos;
        while(tx_list.size() > 1)
        {
            ArrayList<Sha256Hash> next_list = new ArrayList<Sha256Hash>();
            if (tx_list.size() % 2 == 1)
            {
                tx_list.add(tx_list.get(tx_list.size()-1));
            }
            for(int i=0; i<tx_list.size(); i+=2)
            {

                if (magic_pos==i)
                {
                    out_list.add(tx_list.get(i+1));
                }
                if (magic_pos==i+1)
                {
                    out_list.add(tx_list.get(i));
                }

                next_list.add(treeHash(tx_list.get(i), tx_list.get(i+1)));
            }
            magic_pos = magic_pos / 2;
            tx_list=next_list;
        }
        return tx_list.get(0);
        
        

    }

    private static Sha256Hash getInternalMerkleTreeMadness(ArrayList<Sha256Hash> tx_list, int target_pos, int start, int span, ArrayList<Sha256Hash> out_list)
    {
        if (start >= tx_list.size()) return getInternalMerkleTreeMadness(tx_list, target_pos, start-span, span, null);
        if (span == 1)
        {
            return tx_list.get(start);
        }
        int mid = start + span/2;
        if ((start <= target_pos) && (target_pos < (start+span)))
        {

            Sha256Hash first = getInternalMerkleTreeMadness(tx_list,target_pos, start, span/2, out_list);
            Sha256Hash second = getInternalMerkleTreeMadness(tx_list,target_pos, mid, span/2, out_list);
            if (out_list != null)
            {
                if (target_pos < mid)
                {
                    out_list.add(second);

                }
                else
                {
                    out_list.add(first);
                    
                }
            }
            return treeHash(first, second);

        }
        else
        {
            return treeHash(
                getInternalMerkleTreeMadness(tx_list, target_pos, start, span/2, null),
                getInternalMerkleTreeMadness(tx_list, target_pos, mid, span/2, null));
            
        }

    }


  public static NetworkParameters getNetworkParameters(Config config)
  {
    NetworkParameters network_params = MainNetParams.get();
    if (config.getBoolean("testnet"))
    { 
      network_params = TestNet3Params.get();
    }
    return network_params;

  }


  private static Map<Integer, Double> last_fee_map;
  private static long last_fee_map_time;

  public static Map<Integer, Double> getFeeEstimateMap()
  {
    updateFeeMap();
    return last_fee_map;

  }
  private synchronized static void updateFeeMap()
  {
    try
    {
      if (last_fee_map_time + FEE_MAP_UPDATE_TIME < System.currentTimeMillis())
      {
        URL url = new URL("https://ds73ipzb70zbz.cloudfront.net/fee_estimates");
        Scanner scan = new Scanner(url.openStream());
        StringBuilder sb = new StringBuilder();
        while(scan.hasNextLine())
        {
          String line = scan.nextLine();
          sb.append(line);
        }
        scan.close();
        JSONObject obj = new JSONObject(sb.toString());
        JSONObject fees = obj.getJSONObject("fees");

        TreeMap<Integer, Double> map = new TreeMap<>();

        for(int i=1; i<120; i++)
        {
          double v = fees.getDouble("" + i);
          map.put(i, v);
        }
        last_fee_map = map;
        last_fee_map_time = System.currentTimeMillis();
      }
    }
    catch(Throwable t)
    {
      System.out.println("Error getting fee estimates: " + t);
    }

  }
}
