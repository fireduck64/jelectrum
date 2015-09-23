package bloomtime;

import java.io.RandomAccessFile;

import java.io.File;
import java.util.ArrayList;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.util.BitSet;

import org.junit.Assert;

public class LongMappedBuffer
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
    Assert.assertTrue(position >= 0);
    Assert.assertTrue(position + buff.length <= total_size);

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
  }

  public synchronized void putBytes(long position, byte[] buff)
  {
    Assert.assertTrue(position >= 0);
    Assert.assertTrue(position + buff.length <= total_size);

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
  }

  public synchronized void setBit(long bit)
  {
    long file_pos = bit / 8;
    int bit_in_byte = (int)(bit % 8);

    byte[] b = new byte[1];

    getBytes(file_pos, b);

    BitSet bs = BitSet.valueOf(b);
    bs.set(bit_in_byte);
    b = bs.toByteArray();

    putBytes(file_pos, b);
  }

}
