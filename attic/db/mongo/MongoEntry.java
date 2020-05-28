package jelectrum.db.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.Binary;

import org.bitcoinj.core.Sha256Hash;
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
import java.nio.ByteBuffer;
import com.google.protobuf.ByteString;

public class MongoEntry extends BasicDBObject
{
    public MongoEntry(String key, ByteString value)
    {
        super("_id", key);
        saveData(key,value);
    }
    public MongoEntry(String key, String value, String key_name)
    {
        super("_id", key + "." + value);
        append(key_name, key);
        append("v", value);

    }
    private void saveData(String key, ByteString value)
    {
            Binary b = new Binary(org.bson.BSON.B_GENERAL, value.toByteArray());
            append("b",b);
    }
    public static ByteString getValueByte(DBObject o)
    {
      return ByteString.copyFrom((byte[])o.get("b"));
    }
    public static String getValueString(DBObject o)
    {
      return (String) o.get("v");
    }

    public String getKey()
    {
        return getString("_id");
    }
}
