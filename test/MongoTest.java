import org.junit.Test;
import org.junit.Assert;

import com.mongodb.*;

import java.util.Set;

import jelectrum.MongoKey;
import jelectrum.MongoEntry;
import jelectrum.MongoMapSet;

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

        coll.save(new MongoEntry("a","1",true));
        coll.save(new MongoEntry("b","1",true));
        coll.save(new MongoEntry("c","1",true));
        coll.save(new MongoEntry("d","1",true));
        coll.save(new MongoEntry("a","100",true));

        Assert.assertEquals(4, coll.count());

        Assert.assertEquals(1, coll.count(new MongoKey("a")));
        Assert.assertEquals(0, coll.count(new MongoKey("aaa")));

        {
            TestObject to_1 = new TestObject();
            to_1.a = 17;
            coll.save(new MongoEntry("obj", to_1,true));
            to_1.a = 37;
            Assert.assertEquals(37, to_1.a);
        }

        DBObject o = coll.findOne(new MongoKey("obj"));
        TestObject to_2 = (TestObject) MongoEntry.getValue(o,true);
        Assert.assertEquals(17, to_2.a);

        

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

        MongoMapSet<String, String> m = new MongoMapSet<String, String>(coll, true);

        Assert.assertEquals(0, m.size());

        m.add("a","a");
        m.add("a","b");
        m.add("a","c");
        m.add("a","d");

        Set<String> s = m.getSet("a");
        Assert.assertEquals(4, s.size());

        m.add("a","d");
        m.add("a","d");
        m.add("a","d");
        m.add("a","d");
        m.add("a","d");
        m.add("a","d");
        m.add("a","d");

        s = m.getSet("a");
        Assert.assertEquals(4, s.size());
            

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
            coll.save(new MongoEntry("a_" + i ,"1",false),WriteConcern.ACKNOWLEDGED);

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
            bulk.insert(new MongoEntry("a_" + i ,"1",false));

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
