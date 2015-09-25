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

public class LongRandomFile implements LongFile
{
  private RandomAccessFile random_file;

  public LongRandomFile(File f, long len)
    throws IOException
  {
    random_file = new RandomAccessFile(f, "rw");
    random_file.setLength(len);
    
  }

  public synchronized void getBytes(long position, byte[] buff)
  {
    try
    {
      random_file.seek(position);
      random_file.readFully(buff);
    }
    catch(IOException e) { throw new RuntimeException(e);}

  }

  public synchronized void putBytes(long position, byte[] buff)
  {
    try
    {
      random_file.seek(position);
      random_file.write(buff);

    }
    catch(IOException e) { throw new RuntimeException(e);}
  }

  public synchronized void setBit(long bit)
  {
    try
    {
      long t1=System.nanoTime();
      long data_pos = bit / 8;
      int bit_in_byte = (int)(bit % 8);

      byte[] b = new byte[1];
      random_file.seek(data_pos);
      random_file.readFully(b);
      BitSet bs = BitSet.valueOf(b);
      bs.set(bit_in_byte);
      b = bs.toByteArray();

      random_file.seek(data_pos);
      random_file.write(b);


      TimeRecord.record(t1, "long_file_set_bit");

    }
    catch(IOException e) { throw new RuntimeException(e);}
  }

}
