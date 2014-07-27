package jelectrum;

import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;


/**
 *  After solving something using a complex solution:
 *    - A novice would say, look at how clever of a soltuion I made
 *    - A jounryman would say, it was complicated but it is done
 *    - A master would say, I have failed to make it simple
 *
 *  So this is complicated and I have failed to make it simple.
 *  The idea here is that as we import transaction, many addresses
 *  will need multiple updates to the transaction lists for them at 
 *  the same time.  But we can't let them go to the database as the
 *  changes will overwrite one another.  So we need to serialize them.
 *  But that is very slow, so we need to group them into batches that
 *  can happen at all once.  And we need the calling thread to wait until
 *  the batch is done because they need to know it was written.
 *
 */
public class BandedUpdater<K, V>
{
    Map<K, HashSet<V> > base_map;

    Map<K, UpdateGroup> pending_updates;

    HashSet<K> in_progress;

    LinkedBlockingQueue<K> to_check;


     

    public BandedUpdater(Map<K, HashSet<V> > map, int threads)
    {
        base_map = map;
        pending_updates = new HashMap<K, UpdateGroup>(256,0.5f);
        in_progress = new HashSet<K>(256,0.5f);
        to_check = new LinkedBlockingQueue<K>();

        for(int i=0; i<threads; i++)
        {
            new BandedUpdaterThread().start();
        }


    }

    /**
     * Adds an item to the hashset associated with the key.
     * Blocks until the write is complete.
     */
    public int addItem(K key, V item)
    {
        UpdateGroup ug = null;
        synchronized(pending_updates)
        {
            if (pending_updates.containsKey(key))
            {
                ug = pending_updates.get(key);
            }
            else
            {   
                ug = new UpdateGroup();
                pending_updates.put(key, ug);
            }
            ug.items.add(item);
        }
        try
        {
            to_check.put(key);
            ug.await();
        }
        catch(java.lang.InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        return ug.new_size;


    }

    public class BandedUpdaterThread extends Thread
    {
        public BandedUpdaterThread()
        {
            setName("BandedUpdaterThread");
            setDaemon(true);
        }

        public void run()
        {
            while(true)
            {
                K key = null;
                try
                {
                    key = to_check.take();
                    tryKey(key);
                   
                }
                catch(Throwable e)
                {
                    e.printStackTrace();
                    if (key!=null)
                    {
                        try
                        {
                            to_check.put(key);
                        }
                        catch(java.lang.InterruptedException e2)
                        {
                            throw new RuntimeException(e2);
                        }
                    }
                }


            }

        }

        private void tryKey(K key)
            throws java.lang.InterruptedException
        {
            UpdateGroup ug = null;
            synchronized(in_progress)
            {
                if (in_progress.contains(key))
                {
                    return;
                }
                synchronized(pending_updates)
                {
                    ug = pending_updates.get(key);

                    if (ug == null) return;

                    pending_updates.remove(key);
                    in_progress.add(key);
                    
                }


            }
            try
            {
                //We have an update group all to ourself
                //and have marked in progress
                //rock it.

                HashSet<V> old = base_map.get(key);
                HashSet<V> new_set = new HashSet<V>();
                if (old!=null)
                {
                    new_set.addAll(old);
                }
                new_set.addAll(ug.items);
                base_map.put(key, new_set);
                ug.latch.countDown();
                ug.new_size = new_set.size();
                if (ug.items.size() > 10)
                {
                    //jelly.getEventLog().log("Banded " + ug.items.size() + " for key " + key);
                }


            }
            finally
            {
                synchronized(in_progress)
                {
                    in_progress.remove(key);
                }

                to_check.put(key);
            }
 
        }

    }


    public class UpdateGroup
    {
        HashSet<V> items=new HashSet<V>();
        CountDownLatch latch=new CountDownLatch(1);

        int new_size;

        public void await()
        {
            try
            {
                latch.await();
            }
            catch(java.lang.InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

    }

}
