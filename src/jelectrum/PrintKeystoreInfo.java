package jelectrum;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Enumeration;
import java.security.cert.Certificate;

public class PrintKeystoreInfo
{

    public static void main(String args[])
        throws Exception
    {
        new PrintKeystoreInfo(new Config(args[0]));
    }

    public PrintKeystoreInfo(Config config)
        throws Exception
    {
        char ks_pass[] = config.get("keystore_store_password").toCharArray();
        char key_pass[] = config.get("keystore_key_password").toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(new FileInputStream(config.get("keystore_path")), ks_pass);


        Enumeration<String> en = ks.aliases();
        while(en.hasMoreElements())
        {
            String alias = en.nextElement();
            System.out.println("Alias: " + alias);
            Certificate cert = ks.getCertificate(alias);
            System.out.println(cert);
        }    



    }


}
