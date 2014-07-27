package jelectrum;

import java.util.LinkedList;
import java.util.PriorityQueue;

import java.text.DecimalFormat;

public class StatData
{

    private long num=0;
    private long sum_x=0;
    private long sum_x_2=0;
    private long min=1000000000;
    private long max=0;

    public StatData()
    {

    }

    public StatData(long num, long sum_x, long sum_x_2, long min, long max)
    {
        this.num = num;
        this.sum_x = sum_x;
        this.sum_x_2 = sum_x_2;
        this.min = min;
        this.max = max;
    }

    public synchronized void addDataPoint(long x)
    {
        num++;
        sum_x += x;
        sum_x_2 += x*x;

        min = Math.min(min, x);
        max = Math.max(max, x);

    }


	public synchronized void addAllPoints(StatData o)
	{
		num += o.num;
		sum_x += o.sum_x;
		sum_x_2 += o.sum_x_2;

		min = Math.min(min, o.min);
		max = Math.max(max, o.max);

	}

    /**
     * Return duplicate StatData and reset this one
     */
    public synchronized StatData copyAndReset()
    {
        StatData n = new StatData(num, sum_x, sum_x_2, min, max);

        num = 0;
        sum_x = 0;
        sum_x_2 = 0;
        min = 1000000000;
        max = 0;

        return n;

    }

    public synchronized double getMean()
    {
        if (num ==0) return 0.0;
        return (double)sum_x / (double) num;
    }

    public synchronized double getStdDev()
    {
        if (num == 0) return 0.0;

        double e_x = sum_x / num;
        double e_x_2 = sum_x_2 / num;

        return Math.sqrt(e_x_2 - e_x * e_x);
    }

    public synchronized long getMin(){return min;}
    public synchronized long getMax(){return max;}
    public synchronized long getNum(){return num;}


    public synchronized void print(String label, DecimalFormat df)
    {

        System.out.println(getReport(label,df));
    }

    public synchronized String getReport(String label, DecimalFormat df)
    {
        StringBuilder sb= new StringBuilder();

        sb.append(label);
        sb.append(" n: " + df.format(num));
        sb.append(" min: " + df.format(min));
        sb.append(" avg: " + df.format(getMean()));
        sb.append(" max: " + df.format(max));
        sb.append(" stddev: " + df.format(getStdDev()));
    

        return sb.toString();

    }

}
