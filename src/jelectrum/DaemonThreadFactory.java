
package jelectrum;

public class DaemonThreadFactory implements java.util.concurrent.ThreadFactory
{
    public Thread newThread(Runnable r)
    {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    }
}
