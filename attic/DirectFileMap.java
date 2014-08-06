package jelectrum;

import java.util.Map;
import java.util.Set;
import java.util.Collection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class DirectFileMap<K,V> implements Map<K, V>
{
    private String base_path;
    private int stride_size;

    public DirectFileMap(String base, String name, int stride_size)
        throws java.io.IOException
    {
        this(base +"/" + name, stride_size);
    }
    public DirectFileMap(String base_path, int stride_size)
        throws java.io.IOException
    {
        this.base_path = base_path;
        this.stride_size = stride_size;

        new File(base_path).mkdirs();
    }

    public void clear()
    {
        throw new RuntimeException("not implemented - is stupid");
    }


    public boolean containsKey(Object key)
    {
        File f = getFileForObject(key);
        return f.exists();
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
            File f = getFileForObject(key);
            if (!f.exists()) return null;

            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f));
        
            V v = (V) ois.readObject();
            ois.close();
            return v;
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
        File base = new File(base_path);
        if (base.exists())
        {
            if (base.listFiles().length>0) return false;

        }
        return true;
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
        try
        {
            File f = getFileForObject(key);
            File parent = f.getParentFile();
            parent.mkdirs();

            File tmp = File.createTempFile(key.toString(), "tmp", parent);

            V old = get(key);

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tmp));
            out.writeObject(value);
            out.flush();
            out.close();

            tmp.renameTo(f);

            return old;
        }
        catch(java.io.IOException e)
        {
            throw new RuntimeException(e);
        }
 
    }
    public void putAll(Map<? extends K,? extends V> m) 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    public V remove(Object key) 
    {
        V old = get(key);
        if (old != null)
        {

            File f = getFileForObject(key);
            f.delete();

        }
        return old;

    }

    public Collection<V>   values() 
    {
        throw new RuntimeException("not implemented - is stupid");
    }

    

    private File getFileForObject(Object o)
    {
        String s = o.toString();

        File f = new File(base_path);
        int len = s.length();

        String part="";

        for(int i=0; i<len; i++)
        {
            if (part.length() >= stride_size)
            {
                f = new File(f, part);
                part="";
            }
            part=part + s.charAt(i);

        }
        if (part.length() > 0)
        {
            f = new File(f, part);
        }
        return f;




    }

}
