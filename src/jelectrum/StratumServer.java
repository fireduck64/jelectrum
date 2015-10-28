package jelectrum;

import java.lang.Math;

import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import java.util.TreeSet;

import java.net.Socket;
import java.net.ServerSocket;

import org.json.JSONObject;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Block;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;
import java.sql.PreparedStatement;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.security.KeyStore;
import java.io.FileInputStream;

public class StratumServer
{
    private long max_idle_time = 1800L * 1000L * 1000L * 1000L;//30 minutes in nanos
    
    private Map<String, StratumConnection> conn_map=new HashMap<String, StratumConnection>(1024, 0.5f);

    private Config config;
    private NetworkParameters network_params;
    private EventLog event_log;

    private String instance_id;

    private StratumServer server;
    private Jelectrum jelectrum;
    private RateLimit global_rate_limit;

    private int tcp_port = -1;
    private int ssl_port = -1;
    
    public StratumServer(Jelectrum jelectrum, Config config)
    {
        this.jelectrum = jelectrum;

        this.config = config;

        //config.require("tcp_port");
        //config.require("ssl_port");
        if (config.isSet("ssl_port"))
        {
            config.require("keystore_path");
            config.require("keystore_store_password");
            config.require("keystore_key_password");
        }
        if (config.isSet("global_rate_limit"))
        {
          global_rate_limit = new RateLimit(config.getDouble("global_rate_limit"), 2.0);
        }
        server = this;

    }

    public void start()
        throws java.net.SocketException, java.io.IOException, java.security.GeneralSecurityException
    {
        getEventLog().log("SERVER START");

        new TimeoutThread().start();

        if (config.isSet("tcp_port"))
        {
            List<String> ports = config.getList("tcp_port");
            for(String s : ports)
            {
                int port = Integer.parseInt(s);
                if (tcp_port < 0) tcp_port = port;

                ServerSocket ss = new ServerSocket(port, 256);
                ss.setReuseAddress(true);
                new ListenThread(ss).start();
            }
        }
        if (config.isSet("ssl_port"))
        {
            char ks_pass[] = config.get("keystore_store_password").toCharArray();
            char key_pass[] = config.get("keystore_key_password").toCharArray();
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(config.get("keystore_path")), ks_pass);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, key_pass);
            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(kmf.getKeyManagers(), null, null);
            SSLServerSocketFactory ssf = sc.getServerSocketFactory();
    
            List<String> ports = config.getList("ssl_port");

            for(String s : ports)
            {
                int port = Integer.parseInt(s);
                if (ssl_port < 0) ssl_port = port;

                SSLServerSocket ss = (SSLServerSocket)ssf.createServerSocket(port, 256);
                ss.setWantClientAuth(false);
                ss.setNeedClientAuth(false);
                ss.setReuseAddress(true);
                new ListenThread(ss).start();
            }
        }

    }

    public Config getConfig()
    {
        return config;
    }

    public String getInstanceId()
    {
        return instance_id;
    }
    public void setInstanceId(String instance_id)
    {
        this.instance_id = instance_id;
    }

    public void setEventLog(EventLog log)
    {
        this.event_log = log;
    }
    public EventLog getEventLog()
    {
        return event_log;
    }

    public int getTcpPort() {return tcp_port;}
    public int getSslPort() {return ssl_port;}

    public int getConnectionCount()
    {
      synchronized(conn_map)
      {
        return conn_map.size();
      }
    }
    public boolean applyGlobalRateLimit(double bytes)
    {
      if (global_rate_limit == null) return false;
      return global_rate_limit.waitForRate(bytes);
    }

    public NetworkParameters getNetworkParameters(){return network_params;}

    public void setNetworkParameters(NetworkParameters network_params)
    {
        this.network_params = network_params;
    }

    public class ListenThread extends Thread
    {
        private ServerSocket ss;
        public ListenThread(ServerSocket ss)
        {
            this.ss = ss;
            setName("Listen:"+ss);
        }


        public void run()
        {
            System.out.println("Listening on port: " + ss);
            jelectrum.getEventLog().log("Listening on port: " + ss);


                while(ss.isBound())
                {
                    try
                    {
                        Socket sock = ss.accept();
                        sock.setTcpNoDelay(true);

                        String id = UUID.randomUUID().toString();

                        StratumConnection conn = new StratumConnection(jelectrum, server, sock, id);
                        synchronized(conn_map)
                        {
                            conn_map.put(id, conn);
                        }
                    }
                    catch(Throwable t)
                    {
                        t.printStackTrace();
                    }

                }

        }
    }

    public class TimeoutThread extends Thread
    {
        public TimeoutThread()
        {
            setName("TimeoutThread");
            setDaemon(true);
        }

        public void run()
        {   
            while(true)
            {   
                LinkedList<Map.Entry<String, StratumConnection> > lst= new LinkedList<Map.Entry<String, StratumConnection> >();

                synchronized(conn_map)
                {
                    lst.addAll(conn_map.entrySet());
                }
                for(Map.Entry<String, StratumConnection> me : lst)
                {
                    if (!me.getValue().isOpen())
                    {
                        synchronized(conn_map)
                        {
                            conn_map.remove(me.getKey());
                        }
                    }
                    if (me.getValue().getLastNetworkAction() + max_idle_time < System.nanoTime())
                    {
                        jelectrum.getEventLog().log(me.getKey() + " - Closing connection due to inactivity");

                        me.getValue().close();
                        synchronized(conn_map)
                        {
                            conn_map.remove(me.getKey());
                        }
                    }   
                }

                try{Thread.sleep(15000);}catch(Throwable t){}

            }

        }

    }

}
