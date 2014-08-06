
import org.junit.Test;
import org.junit.Assert;

import java.util.TreeMap;
import java.util.HashSet;
import java.util.concurrent.Semaphore;
import java.util.Random;

import jelectrum.BandedUpdater;

public class BandedUpdaterTest
{
    @Test
    public void testBasic()
    {
        TreeMap<String, HashSet<String> > map=new TreeMap<String, HashSet<String>>();

        BandedUpdater<String, String> bu = new BandedUpdater<String,String>(map,2);

        bu.addItem("a","cow");
        bu.addItem("a","wolf");
        bu.addItem("a","tomato");

        Assert.assertEquals(1, map.size());
        Assert.assertEquals(3, map.get("a").size());

        bu.addItem("b","tomato");
        Assert.assertEquals(2, map.size());
        
    }
    @Test
    public void multiThreadTest()
        throws Exception
    {
        TreeMap<String, HashSet<String> > map=new DelayMap<String, HashSet<String>>();

        BandedUpdater<String, String> bu = new BandedUpdater<String,String>(map,16);
        Semaphore sem = new Semaphore(0);

        for(int i=0; i<100; i++)
        {
            new InsertThread(bu, sem).start();
        }
        sem.acquire(100);

        Assert.assertEquals(1, map.size());
        Assert.assertEquals(1000, map.get("a").size());

        
    }

    public class InsertThread extends Thread
    {
        BandedUpdater<String, String> bu;
        Semaphore sem;
        public InsertThread(BandedUpdater<String, String> bu, Semaphore sem)
        {
            this.bu = bu;
            this.sem = sem;

        }
        public void run()
        {
            Random rnd = new Random();
            for(int i=0; i<10; i++)
            {
                bu.addItem("a", "" + rnd.nextDouble());
            }
            sem.release(1);

        }
    }


    public class DelayMap<K,V> extends TreeMap<K,V>
    {
        public DelayMap()
        {
            super();
        }

        private volatile int concur;

        @Override
        public V put(K k, V v)
        {
            concur++;
            for(int i=0; i<10; i++)
            {
                Assert.assertEquals(1,concur);

                try{Thread.sleep(10);}catch(Exception e){}
            }
            concur--;

            return super.put(k,v);
        }

    }

}
