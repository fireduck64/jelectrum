import org.junit.Test;
import org.junit.Assert;

import com.mongodb.*;

import jelectrum.MongoKey;
import jelectrum.MongoEntry;

public class MongoTest implements java.io.Serializable
{

    @Test
    public void testCrud()
        throws java.io.IOException
    {

        MongoClientOptions.Builder opts = MongoClientOptions.builder();
        opts.connectionsPerHost(1000);

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


    public class TestObject implements java.io.Serializable
    {
        int a,b,c,d,e,f,g;


    }

}
