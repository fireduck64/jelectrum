import org.junit.Test;
import org.junit.Assert;

import com.mongodb.*;

import java.util.Set;

import jelectrum.db.mongo.MongoKey;
import jelectrum.db.mongo.MongoEntry;
import jelectrum.db.mongo.MongoMapSet;
import org.bitcoinj.core.Sha256Hash;

public class MongoTest implements java.io.Serializable
{

    @Test
    public void testCrud()
        throws java.io.IOException
    {

        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(100);

        MongoClient mc = new MongoClient("localhost", opts.build());
        DB db = mc.getDB("test");

        DBCollection coll = db.getCollection("crudtest");
        coll.drop();

        coll.save(new MongoEntry("a",TestUtil.randomByteString()));
        coll.save(new MongoEntry("b",TestUtil.randomByteString()));
        coll.save(new MongoEntry("c",TestUtil.randomByteString()));
        coll.save(new MongoEntry("d",TestUtil.randomByteString()));
        coll.save(new MongoEntry("a",TestUtil.randomByteString()));

        Assert.assertEquals(4, coll.count());

        Assert.assertEquals(1, coll.count(new MongoKey("a")));
        Assert.assertEquals(0, coll.count(new MongoKey("aaa")));

        

    }

    @Test
    public void testMapSet()
        throws Exception
    {
        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(100);

        MongoClient mc = new MongoClient("localhost", opts.build());
        DB db = mc.getDB("test");

        DBCollection coll = db.getCollection("mapsettest");
        coll.drop();

        MongoMapSet m = new MongoMapSet(coll);

        m.add("a",TestUtil.randomHash());
        m.add("a",TestUtil.randomHash());
        m.add("a",TestUtil.randomHash());
        m.add("a",TestUtil.randomHash());

        Set<Sha256Hash> s = m.getSet("a",10000);
        Assert.assertEquals(4, s.size());

        Sha256Hash h = TestUtil.randomHash();
        m.add("a",h);
        m.add("a",h);
        m.add("a",h);
        m.add("a",h);
        m.add("a",h);

        s = m.getSet("a",10000);
        Assert.assertEquals(5, s.size());
            

    }

    /*@Test
    public void testUpdate()
        throws java.io.IOException
    {
        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(1000);

        MongoClient mc = new MongoClient("localhost", opts.build());
        DB db = mc.getDB("test");

        DBCollection coll = db.getCollection("updatetest");
        coll.drop();

        coll.save(new MongoEntry("a","1"));

        for(int i=0; i<10; i++)
        {
            DBObject up =  new BasicDBObject("item_" + i,"1");
            up.markAsPartialObject();

            coll.update(new MongoKey("a"), up);

        }


        DBObject o = coll.findOne(new MongoKey("a"));
        System.out.println(o);





    }*/

    @Test
    public void testSaveDirect()
        throws java.io.IOException
    {

        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(100);

        MongoClient mc = new MongoClient("localhost", opts.build());
        DB db = mc.getDB("test");

        DBCollection coll = db.getCollection("savedirect");
        coll.drop();

        long t1 = System.currentTimeMillis();
        for(int i=0; i<10000; i++)
        {
            coll.save(new MongoEntry("a_" + i ,TestUtil.randomByteString()),WriteConcern.ACKNOWLEDGED);

        }
        long t2 = System.currentTimeMillis();
        double sec = (t2 - t1) / 1000.0;

        System.out.println("Items saved in: " + sec + "/s");
        Assert.assertEquals(10000, coll.count());


    }

    @Test
    public void testSaveBulk()
        throws java.io.IOException
    {

        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(100);

        MongoClient mc = new MongoClient("localhost", opts.build());
        DB db = mc.getDB("test");

        DBCollection coll = db.getCollection("savebulk");
        coll.drop();

        long t1 = System.currentTimeMillis();

        BulkWriteOperation bulk = coll.initializeUnorderedBulkOperation();

        for(int i=0; i<10000; i++)
        {
            bulk.insert(new MongoEntry("a_" + i ,TestUtil.randomByteString()));

        }

        bulk.execute(WriteConcern.ACKNOWLEDGED);
        long t2 = System.currentTimeMillis();
        double sec = (t2 - t1) / 1000.0;

        System.out.println("Bulk items saved in: " + sec + "/s");
        Assert.assertEquals(10000, coll.count());


    }

 
    public class TestObject implements java.io.Serializable
    {
        int a,b,c,d,e,f,g;
    }


}
