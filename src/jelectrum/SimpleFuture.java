package jelectrum;

import java.util.concurrent.TimeUnit;


public class SimpleFuture<V> implements java.util.concurrent.Future<V>
{
  private boolean done;
  private V result;
  private RuntimeException re;

  public SimpleFuture()
  {
    done=false;
  }

  public boolean cancel(boolean mayInteruptIfRunning)
  {
    return false;
  }
  public synchronized V get()
  {
    while(!done)
    {
      try
      {
        this.wait();
      }
      catch(InterruptedException e)
      {
        throw new RuntimeException(e);
      }
    }
    if (re != null) throw re;
    return result;
  }

  public synchronized V get(long timeout, TimeUnit unit)
  {
    throw new RuntimeException("no");
  }
  public boolean isCancelled()
  {
    return false;
  }
  public synchronized boolean isDone()
  {
    return done;
  }

  public synchronized void setResult(V res)
  {
    done=true;
    result = res;
    this.notifyAll();
  }
  public synchronized void setException(RuntimeException re)
  {
    done=true;
    this.re = re;
    this.notifyAll();
  }

}
