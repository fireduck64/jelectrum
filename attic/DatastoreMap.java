package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.Collection;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import com.google.protobuf.ByteString;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Value;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.LookupRequest;
import com.google.api.services.datastore.DatastoreV1.LookupResponse;
import com.google.api.services.datastore.DatastoreV1.CommitRequest;
import com.google.api.services.datastore.DatastoreV1.CommitResponse;
import com.google.api.services.datastore.DatastoreV1.Mutation;

import com.google.api.client.http.InputStreamContent;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.google.api.services.storage.Storage;
import java.util.Random;

public class DatastoreMap<K,V> implements Map<K, V>
{
    private String base_key;
    private Datastore ds;
    private Storage bigstore;

    public static final int MAX_DATASTORE_SIZE=1048576;

    public DatastoreMap(Datastore ds, Storage bigstore, String base)
        throws java.io.IOException
    {
        base_key = base;
        this.ds = ds;
        this.bigstore = bigstore;
    }

    public void clear()
    {
        throw new RuntimeException("not implemented - is stupid");
    }


    public boolean containsKey(Object key)
    {
        Key k = getKeyForObject(key);
        //System.out.println("Key: " + k.toString());
        while(true)
        {
            try
            {
                LookupRequest request = LookupRequest.newBuilder().addKey(k).build();
                LookupResponse response = ds.lookup(request);
                if (response.getMissingCount() == 1) {
                    return false;
                }
                return true;
            }
            catch(com.google.api.services.datastore.client.DatastoreException e)
            {
                
                e.printStackTrace();
                try{Thread.sleep(5000);}catch(Throwable t){}
            }
        }

    }
    public boolean containsValue(Object value)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public Set<Map.Entry<K,V>> entrySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public  boolean equals(Object o)
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V get(Object key)
    {
        try
        {
            while(true)
            {
                try
                {
                    long t1 = System.currentTimeMillis();

                    Key k = getKeyForObject(key);
                    LookupRequest request = LookupRequest.newBuilder().addKey(k).build();
                    LookupResponse response = ds.lookup(request);
                    if (response.getMissingCount() == 1) {
                      return null;
                    }

                    Entity obj = response.getFound(0).getEntity();
    
                    Map<String,Value> prop_map = DatastoreHelper.getPropertyMap(obj);
                    //System.out.println("map: " + prop_map);

                    ByteString bs = null;

                    if (prop_map.containsKey("serial_object"))
                    {
    
                        bs = prop_map.get("serial_object").getBlobValue();
                    }
                    else
                    {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        String bigstore_key = prop_map.get("bigstore_key").getStringValue();
                        Storage.Objects.Get getObject =
                                bigstore.objects().get("jelectrum", bigstore_key);
                        getObject.getMediaHttpDownloader().setDirectDownloadEnabled(true);
                        getObject.executeMediaAndDownloadTo(out);

                        bs = ByteString.copyFrom(out.toByteArray());

                    }

                    //System.out.println("Serial object of size: " + bs.size());
    
                    ObjectInputStream ois = new ObjectInputStream(bs.newInput());
            
                    V v = (V) ois.readObject();
                    ois.close();

                    long t2 = System.currentTimeMillis();
                    double sec= (t2 - t1)/1000.0;
                    //System.out.println("Get " + base_key + "/" +key.toString()+ " time: " + sec);
                    /*if (new Random().nextDouble() < 0.01)
                    {
                        Exception e = new RuntimeException("Unlucky bastard");
                        e.printStackTrace();
                    }*/
                    return v;
                }
                catch(com.google.api.services.datastore.client.DatastoreException e)
                {
                    e.printStackTrace();
                    try{Thread.sleep(5000);}catch(Throwable t){}
                }
            }

        }
        catch(java.io.IOException e)
        {
            throw new RuntimeException(e);
        }
        catch(java.lang.ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

    }
    public int hashCode() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public boolean isEmpty()
    {
        throw new RuntimeException("not implemented - is stupid");
    }
    public int size()
    {
        throw new RuntimeException("not implemented - is stupid");
    }


    public  Set<K>  keySet()
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V put(K key, V value) 
    {
        while(true)
        {
            try
            {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
                Key k = getKeyForObject(key);

                ByteArrayOutputStream b_out = new ByteArrayOutputStream();


                ObjectOutputStream out = new ObjectOutputStream(b_out);
                out.writeObject(value);
                out.flush();
                out.close();
            
                ByteString bs = ByteString.copyFrom(b_out.toByteArray());
                //System.out.println("Putting object of size: " + bs.size());

                String bigstore_key = null;



                if (bs.size() >= MAX_DATASTORE_SIZE)
                {
                    InputStreamContent mediaContent = new InputStreamContent(
                            "application/octet-stream",bs.newInput());
                    mediaContent.setLength(bs.size());

                    Storage.Objects.Insert insertObject =
                            bigstore.objects().insert("jelectrum", null, mediaContent);

                    bigstore_key = base_key + "/" + key.toString();
                    insertObject.setName(bigstore_key);
                    System.out.println("Saving to bigstore: " + bigstore_key);
                    insertObject.execute();
                    System.out.println("Saved to bigstore: " + bigstore_key);

                }

                Entity.Builder e = Entity.newBuilder();

                e.setKey(k);
                e.addProperty(DatastoreHelper.makeProperty("updated", DatastoreHelper.makeValue(sdf.format(new Date())).setIndexed(false)));
                e.addProperty(DatastoreHelper.makeProperty("size", DatastoreHelper.makeValue(bs.size()).setIndexed(false)));

                if (bigstore_key == null)
                {
                    e.addProperty(DatastoreHelper.makeProperty("serial_object", DatastoreHelper.makeValue(bs).setIndexed(false)));
                }
                else
                {
                    e.addProperty(DatastoreHelper.makeProperty("bigstore_key", DatastoreHelper.makeValue(bigstore_key).setIndexed(false)));
                }
                   
            
                V old = get(key);

                long t1 = System.currentTimeMillis();
                CommitRequest.Builder commitRequest = CommitRequest.newBuilder();

                commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
                if (old==null)
                {
                    commitRequest.setMutation(Mutation.newBuilder().addInsert(e.build()));
                }
                else
                {
                    commitRequest.setMutation(Mutation.newBuilder().addUpdate(e.build()));

                }

                CommitResponse response = ds.commit(commitRequest.build());

                long t2 = System.currentTimeMillis();

                    double sec= (t2 - t1)/1000.0;
                    //System.out.println("Put time: " + sec);

                return old;
            }
            catch(com.google.api.services.datastore.client.DatastoreException e)
            {
                e.printStackTrace();
                try{Thread.sleep(5000);}catch(Throwable t){}
            }
            catch(java.io.IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V remove(Object key) 
    {
        throw new RuntimeException("not implemented - is stupid");

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    private Key getKeyForObject(Object o)
    {
        return DatastoreHelper.makeKey(base_key, o.toString()).build();
    }


}
