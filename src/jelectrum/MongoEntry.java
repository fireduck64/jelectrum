package jelectrum;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.Binary;

import com.google.bitcoin.core.Sha256Hash;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Scanner;

public class MongoEntry extends BasicDBObject
{
    public MongoEntry(String key, Object value, boolean compress)
    {
        super("_id", key);
        saveData(key,value,compress);
    }
    public MongoEntry(String key, Object value, boolean compress, String key_name)
    {
        super("_id", key + "." + value.toString());
        append(key_name, key);
        saveData(key,value,compress);

    }
    private void saveData(String key, Object value, boolean compress)
    {
        try
        {

            boolean saved_as_set=false;
            if (value instanceof Set)
            {
                Set hs = (Set)value;
                if (hs.size() > 0)
                {
                    if (hs.iterator().next() instanceof Sha256Hash)
                    {

                        StringBuilder sb=new StringBuilder();
                        for(Object o : hs)
                        {       
                            if (sb.length() > 0)
                                sb.append(' ');
                            sb.append(o.toString());
                        }
                        append("hashset", sb.toString());
                        saved_as_set=true;

                    }
                }


            }
            
            if(!saved_as_set)
            {



                ByteArrayOutputStream b_out = new ByteArrayOutputStream();
                OutputStream i_out = b_out;
                if (compress)
                {
                    i_out = new GZIPOutputStream(b_out);
                }
                ObjectOutputStream o_out = new ObjectOutputStream(i_out);
                o_out.writeObject(value);
                o_out.flush();
                o_out.close();

                Binary b = new Binary(org.bson.BSON.B_GENERAL, b_out.toByteArray());

                int size = b_out.toByteArray().length;
                append("data", b);
                append("size", size);
            }


            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

            append("time", sdf.format(new Date()));
            if (value instanceof Collection)
            {
                Collection c = (Collection)value;
                    append("collection_size", c.size());
            }



        }
        catch(java.io.IOException e)
        {
            throw new RuntimeException(e);
        }
       
    }
    public static Object getValue(DBObject o, boolean compress)
    {
        try
        {
            if (o.containsField("hashset"))
            {
                HashSet<Sha256Hash> hs = new HashSet<Sha256Hash>();
                Scanner scan = new Scanner((String)o.get("hashset"));
                while(scan.hasNext())
                {
                    Sha256Hash h = new Sha256Hash(scan.next());
                    hs.add(h);
                }
                return hs;

            }
            else
            {
                byte[]b = (byte[])o.get("data");
                ByteArrayInputStream b_in = new ByteArrayInputStream(b);
                InputStream i_in = b_in;
                if (compress)
                {
                    i_in = new GZIPInputStream(b_in);
                }
                ObjectInputStream o_in = new ObjectInputStream(i_in);

                return o_in.readObject();
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

    public String getKey()
    {
        return getString("_id");
    }
}
