package jelectrum;

import java.net.InetAddress;


public class PeerInfo implements java.io.Serializable
{
  public PeerInfo()
  {
    learned_time = System.currentTimeMillis();
  }
  String lastAddress;
  String hostname;
  int ssl_port;
  int tcp_port;
  String server_version;
  String protocol_min;
  String protocol_max;
  int pruning;
  boolean self_info;

  volatile long last_checked;
  volatile long last_passed;
  volatile long learned_time;

  public boolean include()
  {
    if (self_info) return true;

    // if never checked
    if (last_checked == 0) return false;

    // if didn't pass last time
    if (last_passed <= last_checked) return false;

    if (last_passed < System.currentTimeMillis() - PeerManager.RECHECK_TIME*2) return false;

    return true;
  }
  public boolean shouldCheck()
  {
    if (self_info) return false;
    if (last_checked > System.currentTimeMillis() - PeerManager.RECHECK_TIME) return false;

    return true;
  }

  public boolean shouldDelete()
  {
    if (self_info) return false;
    if (learned_time > System.currentTimeMillis() - PeerManager.FORGET_TIME) return false;

    return true;
  }

  public String getKey()
  {
    return hostname + "/" + ssl_port + "/" + tcp_port;
  }

  public void updateAddress()
		throws java.net.UnknownHostException
  {
  	if (hostname.endsWith(".onion"))
    { 
    	lastAddress = hostname;
    }
    else
    { 
    	lastAddress = InetAddress.getAllByName(hostname)[0].getHostAddress();
    }

  }

}
