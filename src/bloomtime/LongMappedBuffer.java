package bloomtime;

import java.io.RandomAccessFile;

import java.io.File;
import java.util.ArrayList;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.util.BitSet;
import jelectrum.TimeRecord;

import org.junit.Assert;

public class LongMappedBuffer implements LongFile
{
  public static final long MAP_SIZE = Integer.MAX_VALUE;
  private ArrayList<MappedByteBuffer> map_list;
  private long total_size;

  public LongMappedBuffer(File f, long total_size)
    throws IOException
  {
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    FileChannel chan = raf.getChannel();

    this.total_size = total_size;

    map_list=new ArrayList<>();

    long opened = 0;
    while(opened < total_size)
    {
      long len = Math.min(total_size - opened, MAP_SIZE);
      MappedByteBuffer buf = chan.map(FileChannel.MapMode.READ_WRITE, opened, len);

      opened += len;

      map_list.add(buf);
    }
  }

  public synchronized void getBytes(long position, byte[] buff)
  {
    long t1 = System.nanoTime();

    //Assert.assertTrue(position >= 0);
    //Assert.assertTrue(position + buff.length <= total_size);

    int to_read=buff.length;

    int start_file = (int) (position / MAP_SIZE);
    int start_offset = (int) (position % MAP_SIZE);

    MappedByteBuffer map = map_list.get(start_file);

    map.position(start_offset);
    int len = Math.min(to_read, (int) (MAP_SIZE - start_offset));

    map.get(buff, 0, len);
    if (len < to_read)
    {
      map = map_list.get(start_file + 1);
      map.position(0);
      map.get(buff, len, to_read - len);
    }
    TimeRecord.record(t1, "long_map_get_bytes");
  }

  public synchronized void putBytes(long position, byte[] buff)
  {
    long t1 = System.nanoTime();
    //Assert.assertTrue(position >= 0);
    //Assert.assertTrue(position + buff.length <= total_size);

    int to_write=buff.length;

    int start_file = (int) (position / MAP_SIZE);
    int start_offset = (int) (position % MAP_SIZE);

    MappedByteBuffer map = map_list.get(start_file);

    map.position(start_offset);
    int len = Math.min(to_write, (int) (MAP_SIZE - start_offset));

    map.put(buff, 0, len);
    if (len < to_write)
    {
      map = map_list.get(start_file + 1);
      map.position(0);
      map.put(buff, len, to_write - len);
    }
    TimeRecord.record(t1, "long_map_put_bytes");
  }

  public synchronized void setBit(long bit)
  {
    long t1=System.nanoTime();
    long data_pos = bit / 8;
    int file = (int) (data_pos / MAP_SIZE);
    int file_offset = (int) (data_pos % MAP_SIZE);

    int bit_in_byte = (int)(bit % 8);

    byte[] b = new byte[1];
    MappedByteBuffer map = map_list.get(file);

    b[0]=map.get(file_offset);

    BitSet bs = BitSet.valueOf(b);
    if (!bs.get(bit_in_byte))
    {
      bs.set(bit_in_byte);
      b = bs.toByteArray();

      map.put(file_offset, b[0]);
    }

    TimeRecord.record(t1, "long_map_set_bit");
  }

}
