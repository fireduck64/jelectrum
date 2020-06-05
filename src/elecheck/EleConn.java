package elecheck;

import java.io.PrintStream;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONArray;
import java.net.Socket;
import java.util.Scanner;

import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import net.minidev.json.parser.JSONParser;

  public class EleConn
  {
    public static final int REPLY_TIMEOUT_SEC=60;
    public static final boolean VERBOSE=true;

    public EleConn(Socket sock)
      throws Exception
    {
      this.sock = sock;
      in_scan = new Scanner(sock.getInputStream());
      p_out = new PrintStream(sock.getOutputStream());

      new ConnReadThread().start();

    }

    private Socket sock;
    private Scanner in_scan;
    private PrintStream p_out;
    int req_id = 0;
    private TreeMap<Integer, LinkedBlockingQueue<JSONObject> > msg_map = new TreeMap<>();

    public JSONObject request(String method)
    {
      return request(method, null);
    }
    public synchronized JSONObject request(String method, JSONArray params)
    {
      int id = req_id++;

      JSONObject req = new JSONObject();
      req.put("method", method);
      req.put("id", id);
      if (params != null)
      {
        req.put("params", params);
      }
      
      if (VERBOSE)
      {
        System.out.println("Out: " + req.toJSONString());
      }
      p_out.println(req.toJSONString());

      return readReply(id);

    }


    public JSONObject readReply(int id)
    {
      LinkedBlockingQueue<JSONObject> q = getQueue(id);
      try
      {
        return q.poll(REPLY_TIMEOUT_SEC, TimeUnit.SECONDS);
      }
      catch(InterruptedException e){throw new RuntimeException(e);}

    }

    public LinkedBlockingQueue<JSONObject> getQueue(int id)
    {
      synchronized(msg_map)
      {
        if (!msg_map.containsKey(id))
        {
          msg_map.put(id, new LinkedBlockingQueue<JSONObject>());
        }
        return msg_map.get(id);
      }
    }

    public void close()
    {
      try
      {
        sock.close();
      }
      catch(java.io.IOException e)
      {}
    }

    public class ConnReadThread extends Thread
    {
      public ConnReadThread()
      {
        setDaemon(true);
      }

      public void run()
      {
        try
        {
          JSONParser parser = new JSONParser(JSONParser.MODE_STRICTEST);

          while(sock.isConnected())
          {
            String line = in_scan.nextLine();
            if (VERBOSE)
            {
              System.out.println("In: " + line);
            }
            JSONObject o = (JSONObject) parser.parse(line);

            int id = (int)o.get("id");

            getQueue(id).put(o);
          }
        }
        catch(Exception e)
        {
          System.out.println(e);
          close();
        }

      }

    }

  }

