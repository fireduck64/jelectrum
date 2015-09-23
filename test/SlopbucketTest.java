
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import jelectrum.EventLog;
import java.io.File;
import slopbucket.Slopbucket;
import com.google.protobuf.ByteString;

public class SlopbucketTest
{
  private static Slopbucket slop;

  @BeforeClass
  public static void load()
  {
    File f = new File("/var/ssd/clash/slop");
    f.mkdirs();
    

    new File("/var/ssd/clash/slop/data_0000").delete();
    new File("/var/ssd/clash/slop/data_0001").delete();
    new File("/var/ssd/clash/slop/data_0002").delete();
    new File("/var/ssd/clash/slop/data_0003").delete();
    new File("/var/ssd/clash/slop/data_0004").delete();
    new File("/var/ssd/clash/slop/data_0005").delete();
    new File("/var/ssd/clash/slop/data_0006").delete();
    new File("/var/ssd/clash/slop/data_0007").delete();
    new File("/var/ssd/clash/slop/data_0008").delete();

    slop=new Slopbucket(f, new EventLog(System.out));

  }

  @Test
  public void testTroughs()
  {
    Assert.assertEquals(1, slop.getTroughMap().size());

    slop.addTrough("test");
    Assert.assertEquals(2, slop.getTroughMap().size());

    slop.addTrough("test");
    Assert.assertEquals(2, slop.getTroughMap().size());
    slop.addTrough("zoinks");
    Assert.assertEquals(3, slop.getTroughMap().size());
    System.out.println(slop.getTroughMap());
    
  }

  @Test
  public void testPut()
  {
    slop.addTrough("put");
    for(int i=0; i<1000000; i++)
    {
      slop.putKeyValue("put", TestUtil.randomByteString(), TestUtil.randomByteString());
    }
  }

  @Test
  public void testPutGet()
  {
    slop.addTrough("put");
    ByteString key = TestUtil.randomByteString();
    ByteString data_0 = TestUtil.randomByteString();
    ByteString data_1 = TestUtil.randomByteString();
    
    Assert.assertNull(slop.getKeyValue("put", key));

    slop.putKeyValue("put", key, data_0);
    Assert.assertEquals(data_0, slop.getKeyValue("put", key));

    slop.putKeyValue("put", key, data_1);
    Assert.assertEquals(data_1, slop.getKeyValue("put", key));
  }


}
