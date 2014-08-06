package jelectrum;

import java.util.Map;
import java.util.HashSet;
import com.google.bitcoin.core.Sha256Hash;
import com.google.bitcoin.core.Transaction;
import com.google.bitcoin.core.StoredBlock;
import com.google.bitcoin.core.Block;
import com.google.api.services.datastore.client.DatastoreOptions;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreFactory;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;

import com.google.api.services.storage.Storage;
import java.io.File;

public class JelectrumDBCloudData extends JelectrumDB
{
    private Datastore ds;
    private Storage bigstore;

    public JelectrumDBCloudData(Config conf)
        throws Exception
    {
        super(conf);

        conf.require("google_api_keyfile");
        conf.require("google_api_account");
        conf.require("google_project");

        ds = DatastoreFactory.get().create(getDatastoreOptions().build());

        openStorage();

        String file_base = conf.get("db_direct_path");

        tx_map = new DatastoreMap<Sha256Hash, SerializedTransaction>(ds,bigstore,"tx_map");
        address_to_tx_map = new DatastoreMap<String, HashSet<Sha256Hash> >(ds,bigstore,"address_to_tx_map");
        //block_store_map = new DatastoreMap<Sha256Hash, StoredBlock>(ds,bigstore,"block_store_map");
        block_store_map = new DirectFileMap<Sha256Hash, StoredBlock>(file_base,"block_store_map",4);
        special_block_store_map = new DatastoreMap<String, StoredBlock>(ds,bigstore,"special_block_store_map");
        block_map = new DatastoreMap<Sha256Hash, SerializedBlock>(ds,bigstore,"block_map");
        tx_to_block_map = new DatastoreMap<Sha256Hash, HashSet<Sha256Hash> >(ds,bigstore,"tx_to_block_map");

    }

    public Map<Sha256Hash, StoredBlock> getBlockStoreMap()
    {   
        return block_store_map;
    }

    public Map<String, StoredBlock> getSpecialBlockStoreMap()
    {   
        return special_block_store_map;
    }

    public Map<Sha256Hash,SerializedTransaction> getTransactionMap()
    {   
        return tx_map;
    }
    public Map<Sha256Hash, SerializedBlock> getBlockMap()
    {   
        return block_map;
    }

    public Map<String, HashSet<Sha256Hash> > getAddressToTxMap()
    {   
        return address_to_tx_map;
    }

    public Map<Sha256Hash, HashSet<Sha256Hash> > getTxToBlockMap()
    {   
        return tx_to_block_map;
    }


    private DatastoreOptions.Builder getDatastoreOptions()
        throws java.security.GeneralSecurityException, java.io.IOException
    {
        DatastoreOptions.Builder options = new DatastoreOptions.Builder();

        GoogleCredential cred = openCredential();

        options.dataset(conf.get("google_project"));
        //options.host("https://www.googleapis.com/datastore/v1/datasets/hhtt-nodes-184");
        options.credential(cred);

        return options;

    }

    private GoogleCredential openCredential()
        throws java.security.GeneralSecurityException, java.io.IOException
    {
       NetHttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
        JacksonFactory jsonFactory = new JacksonFactory();

        GoogleCredential cred = new GoogleCredential.Builder()
            .setTransport(transport)
            .setJsonFactory(jsonFactory)
            .setServiceAccountId(conf.get("google_api_account"))
            .setServiceAccountScopes(DatastoreOptions.SCOPES)
            .setServiceAccountPrivateKeyFromP12File(new File(conf.get("google_api_keyfile")))
            .build();


        return cred;


    }

    private void openStorage()
        throws java.security.GeneralSecurityException, java.io.IOException
    {
        NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredential cred = openCredential();
        bigstore = new Storage.Builder(httpTransport, new JacksonFactory(), cred).setApplicationName("jelectrum/1.0").build();


    }

    public Storage getStorage()
    {
        return bigstore;
    }
    


}
