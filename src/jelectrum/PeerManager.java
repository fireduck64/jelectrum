
package jelectrum;

import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map;
import java.net.InetAddress;

import org.json.JSONObject;
import org.json.JSONArray;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Scanner;

import javax.net.ssl.SSLSocket;
import javax.net.SocketFactory;
import java.net.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.io.PrintStream;

public class PeerManager 
{
  private Jelectrum jelly;

  // protected by synchronized
  private TreeMap<String, PeerInfo> knownPeers;

  // immutable
  private final LinkedList<PeerInfo> selfPeers;

  // replaced all the damn time
  private volatile JSONArray lastPeerList;

  public static long RECHECK_TIME = 8L * 3600L* 1000L; //8 hours
  //public static long RECHECK_TIME = 3600 * 1000L; //1 hr
  public static long FORGET_TIME = 7L * 86400L * 1000L; //1 week

  private final String genesis_hash;

  public PeerManager(Jelectrum jelly) 
  {
    this.jelly = jelly;
    genesis_hash = jelly.getNetworkParameters().getGenesisBlock().getHash().toString();
    knownPeers = new TreeMap<>();
    selfPeers = new LinkedList<>();
  }

  public void start()
  {

    Config config = jelly.getConfig();

    tryLoadPeerDB();

    addPeer("h.1209k.com",50001,50002);
    addPeer("b.1209k.com",50001,50002);
    addPeer("electrum.vom-stausee.de",0,50002);
    addPeer("mashtk6hmnysevfj.onion",50001,0);
    addPeer("luggsqxihfzqjnwm.onion",443,0);
    addPeer("ecdsa.net", 0, 110);
    addPeer("electrum.hsmiths.com", 50001, 50002);

    if (config.isSet("advertise_host"))
    {
      for(String host : config.getList("advertise_host"))
      {
        try
        {
          PeerInfo self_info = new PeerInfo();
          self_info.self_info=true;
          self_info.hostname = host;
          self_info.pruning = 0;
          self_info.protocol_min = "0.10";
          self_info.protocol_max = StratumConnection.PROTO_VERSION;
          self_info.server_version = StratumConnection.PROTO_VERSION + "/j/" + StratumConnection.JELECTRUM_VERSION;

          self_info.updateAddress();

          if (config.isSet("tcp_port"))
          { 
            self_info.tcp_port = jelly.getStratumServer().getTcpPort();
          }
          if (config.isSet("ssl_port"))
          { 
            self_info.ssl_port = jelly.getStratumServer().getSslPort();
          }
          synchronized(knownPeers)
          {
            knownPeers.put(self_info.getKey(), self_info);
          }
          selfPeers.add(self_info);
        }
        catch(Exception e)
        {
          jelly.getEventLog().alarm(e);
        }
      }
    }
    synchronized(knownPeers) 
    {
      jelly.getEventLog().log("Known discovery peers: " + knownPeers.size());
    }
    new PeerMaintThread().start();
    
  }

  private void addPeer(String host, int tcp, int ssl)
  {
    PeerInfo peer = new PeerInfo();
    peer.hostname = host;
    peer.tcp_port = tcp;
    peer.ssl_port = ssl;
    addPeerInternal(peer);

  }

  public JSONArray getPeers()
  {
    JSONArray peerList = lastPeerList;
    if (peerList == null)
    {
      peerList = updatePeerList();
    }
    return peerList;
  }

  private JSONArray updatePeerList()
  {
    return lastPeerList = populatePeerSubscribe();
  }

  private JSONArray populatePeerSubscribe()
  {
    JSONArray result_array = new JSONArray();

    synchronized(knownPeers)
    {

      for(PeerInfo peer : knownPeers.values())
      {
        if (peer.include())
        {
          JSONArray peer_array = new JSONArray();

          peer_array.put(peer.lastAddress);
          peer_array.put(peer.hostname);

          JSONArray info_array = new JSONArray();
          info_array.put("v" + peer.protocol_max);
          if (peer.pruning > 0)
          {
          info_array.put("p" + peer.pruning);
          }
          if (peer.tcp_port > 0)
          {
            info_array.put("t" + peer.tcp_port);
          }
          if (peer.ssl_port > 0)
          {
            info_array.put("s" + peer.ssl_port);
          }

          peer_array.put(info_array);

          result_array.put(peer_array);
        }

      }
    }

    return result_array;
    

  }

  public JSONObject getServerFeatures()
    throws org.json.JSONException
  {
    JSONObject result = new JSONObject();

    PeerInfo peer = null;
    for(PeerInfo p : selfPeers) { peer = p; }
    if (peer != null)
    {
      result.put("genesis_hash", genesis_hash);

      JSONObject hostArray = new JSONObject();
      for(PeerInfo p : selfPeers)
      {
        JSONObject h = new JSONObject();
        JSONObject h2 = new JSONObject();
        if (p.tcp_port > 0) {
          h2.put("tcp_port", p.tcp_port);
        }
        if (p.ssl_port > 0) {
          h2.put("ssl_port", p.ssl_port);
        }
        hostArray.put(p.hostname, h2);
      }
      result.put("hosts", hostArray);

      result.put("protocol_max", peer.protocol_max);
      result.put("protocol_min", peer.protocol_min);
      result.put("server_version", peer.server_version);
    }

    return result;

  }

  public void addPeers(JSONArray params)
    throws org.json.JSONException
  {
    for(int i=0; i<params.length(); i++)
    {
      JSONObject features = params.getJSONObject(i);
      JSONObject hosts = features.getJSONObject("hosts");
      for(String hostname : JSONObject.getNames(hosts))
      {
        JSONObject host = hosts.getJSONObject(hostname);

        PeerInfo peer = new PeerInfo();
        peer.hostname = hostname;

        if (host.has("tcp_port")) { peer.tcp_port = host.getInt("tcp_port"); }
        if (host.has("ssl_port")) { peer.ssl_port = host.getInt("ssl_port"); }

        addPeerInternal(peer);

      }

    }
  }

  private void addPeerInternal(PeerInfo peer)
  {
    String key = peer.getKey();
    synchronized(knownPeers)
    {
      if (!knownPeers.containsKey(key))
      {
        knownPeers.put(key, peer);
        jelly.getEventLog().log("Learned new discovery peer: " + key);
      }
      else
      {
        knownPeers.get(key).learned_time = System.currentTimeMillis();
      }
    }

  }


  private void savePeerDB()
  {
    synchronized(knownPeers)
    {
      jelly.getDB().getSpecialObjectMap().put("PeerManager_PeerDB", knownPeers);
    }
  }
  private void tryLoadPeerDB()
  { 
    try
    {
      TreeMap<String, PeerInfo> dbPeers = (TreeMap<String, PeerInfo>)jelly.getDB().getSpecialObjectMap().get("PeerManager_PeerDB");
      synchronized(knownPeers)
      {
        knownPeers.putAll(dbPeers);
      }
    }
    catch(Throwable t)
    {
      jelly.getEventLog().log("Error loading peer db.  Starting fresh: " + t.toString());
    }

  }

  public class PeerMaintThread extends Thread
  {
    public PeerMaintThread()
    {
      setDaemon(true);
      setName("PeerMaintThread");

    }

    public void run()
    {
      while(true)
      {
        try
        {
          Thread.sleep(10000L);
          runPass();

        }
        catch(Throwable t)
        {
          jelly.getEventLog().alarm(t);
        }
      }
    }

    private void runPass()
    {
      boolean changes=false;

      TreeSet<String> deleteList=new TreeSet<>();
      LinkedList<PeerInfo> checkList = new LinkedList<>();

      synchronized(knownPeers)
      {
        for(Map.Entry<String, PeerInfo> me : knownPeers.entrySet())
        {
          String key = me.getKey();
          PeerInfo peer = me.getValue();
          if (peer.shouldDelete()) deleteList.add(key);
        }

        for(String key : deleteList)
        {
          jelly.getEventLog().log("Removing stale discovery peer " + key);
          knownPeers.remove(key);
          changes=true;
        }



        for(Map.Entry<String, PeerInfo> me : knownPeers.entrySet())
        {
          String key = me.getKey();
          PeerInfo peer = me.getValue();

          if (peer.shouldCheck())
          {
            checkList.add(peer);
          }
        }
      }

      Collections.shuffle(checkList, new java.util.Random());
      if (checkList.size() > 0)
      {
        jelly.getEventLog().log("Discovery peers that need to be checked: " + checkList.size());

        PeerInfo peer = checkList.get(0);
        checkPeer(peer);
        changes=true;
      }

      if (changes)
      {
        savePeerDB();
        lastPeerList=null;
      }

    }

  }

  private void checkPeer(PeerInfo peer)
  {
    jelly.getEventLog().log("Checking discovery peer: " + peer.getKey());
    try
    {
      peer.last_checked = System.currentTimeMillis();
      if (checkPeerInternal(peer))
      {
        peer.last_passed = System.currentTimeMillis();
        jelly.getEventLog().log("Peer check passed: " + peer.getKey());
        return;
      }
    }
    catch(Throwable t)
    {
      jelly.getEventLog().log("Exception in peer check: " + t.toString());
    }
    jelly.getEventLog().log("Peer check failed: " + peer.getKey());


  }

  private boolean checkPeerInternal(PeerInfo peer)
    throws Exception
  {
      peer.updateAddress();

      Socket sock = openSocket(peer);
      sock.setTcpNoDelay(true);
      sock.setSoTimeout(15000);

      Scanner scan = new Scanner(sock.getInputStream());
      PrintStream out = new PrintStream(sock.getOutputStream());

      JSONObject serverfeatures = getServerFeatures(out, scan);

      String hash = serverfeatures.getString("genesis_hash");
      if(!genesis_hash.equals(hash))
      {
        throw new Exception("Expected " + genesis_hash + " got " + hash);
      }
      peer.protocol_max = serverfeatures.getString("protocol_max");
      peer.protocol_min = serverfeatures.getString("protocol_min");
      peer.server_version = serverfeatures.getString("server_version");
      if (serverfeatures.isNull("pruning")) peer.pruning = 0;
      else
      {
        peer.pruning = serverfeatures.getInt("pruning");
      }
      addRemotePeers(out, scan);
      addSelfToPeer(out, scan);

      sock.close();

      return true;

  }

  private JSONObject getServerFeatures(PrintStream out, Scanner scan)
    throws org.json.JSONException
  {
    JSONObject request = new JSONObject();
    request.put("id","server_req");
    request.put("method", "server.features");
    request.put("params",new JSONArray());

    out.println(request.toString(0));
    out.flush();

    JSONObject reply = new JSONObject(scan.nextLine());
    return reply.getJSONObject("result");

  }

   private void addRemotePeers(PrintStream out, Scanner scan)
    throws org.json.JSONException
  {
    JSONObject request = new JSONObject();
    request.put("id","get_peer");
    request.put("method", "server.peers.subscribe");
    request.put("params",new JSONArray());

    out.println(request.toString(0));
    out.flush();

    JSONObject reply = new JSONObject(scan.nextLine());

    JSONArray peerList = reply.getJSONArray("result");

    for(int i=0; i<peerList.length(); i++)
    {
      JSONArray p = peerList.getJSONArray(i);
      String host = p.getString(1);
      JSONArray param = p.getJSONArray(2);
      int tcp_port=0;
      int ssl_port=0;
      for(int j=0; j<param.length(); j++)
      {
        String str = param.getString(j);
        if (str.startsWith("s"))
        {
          ssl_port = Integer.parseInt(str.substring(1));
        }
        if (str.startsWith("t"))
        {
          tcp_port = Integer.parseInt(str.substring(1));
        }

      }
      if (tcp_port + ssl_port > 0)
      {
        addPeer(host, tcp_port, ssl_port);
      }

    }

  }

 

  private void addSelfToPeer(PrintStream out, Scanner scan)
    throws org.json.JSONException
  { 
    JSONArray peersToAdd = new JSONArray();
    peersToAdd.put(getServerFeatures());
    
    JSONObject request = new JSONObject();
    request.put("id","add_peer");
    request.put("method", "server.add_peer");
    request.put("params", peersToAdd);

    out.println(request.toString(0));
    out.flush();

    //JSONObject reply = new JSONObject(scan.nextLine());
    //jelly.getEventLog().log(reply.toString());


  }

  private Socket openSocket(PeerInfo peer)
    throws Exception
  {
    Socket sock = null;
    if (peer.hostname.endsWith(".onion"))
    {
      int port = peer.tcp_port;
      if (port == 0) throw new Exception("Can only use TCP with onion");

      Proxy p = new Proxy(Proxy.Type.SOCKS, new InetSocketAddress("localhost", 9050));

      sock = new Socket(p);
      sock.connect(new InetSocketAddress(peer.hostname, port));

      return sock;

    }
    if (peer.ssl_port != 0)
    {
      int port = peer.ssl_port;
      SocketFactory sock_factory = new TrustEraser().getFactory();
      SSLSocket ssl_sock = (SSLSocket)sock_factory.createSocket(peer.hostname, port);
      ssl_sock.startHandshake();
      sock = ssl_sock;
      return sock;
    }
    if (peer.tcp_port != 0)
    {
      sock = new Socket(peer.hostname, peer.tcp_port);
      return sock;
    }

    throw new Exception("Unable to find connection method");


  }

}
