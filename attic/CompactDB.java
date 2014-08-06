package jelectrum;

public class CompactDB
{
    public static void main(String args[]) throws Exception
    {
        new CompactDB(new Config(args[0]));

    }

    public CompactDB(Config config)
    {
        JelectrumDB db = new JelectrumDBMapDB(config);

        db.compact();
        db.commit();
        db.close();

    }

}
