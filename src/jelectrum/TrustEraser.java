package jelectrum;

import java.security.*;
import javax.net.ssl.*;
import java.security.cert.X509Certificate;

public class TrustEraser
{
  private SSLContext sc;
  private SSLSocketFactory factory;

  public TrustEraser()
  {
    // Create a trust manager that does not validate certificate chains
    TrustManager[] trustAllCerts = new TrustManager[] { 
        new X509TrustManager() {     
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
                return new X509Certificate[0];
            } 
            public void checkClientTrusted( 
                java.security.cert.X509Certificate[] certs, String authType) {
                } 
            public void checkServerTrusted( 
                java.security.cert.X509Certificate[] certs, String authType) {
            }
        } 
    }; 

    // Install the all-trusting trust manager
    try {
        sc = SSLContext.getInstance("SSL"); 
        sc.init(null, trustAllCerts, new java.security.SecureRandom()); 
        factory = sc.getSocketFactory();
    } catch (GeneralSecurityException e) {
      throw new RuntimeException(e);
    } 

  }

  public SSLSocketFactory getFactory()
  {
    return factory;
  }

}
