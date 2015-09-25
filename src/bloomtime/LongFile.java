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

public interface LongFile
{

  public void getBytes(long position, byte[] buff);

  public void putBytes(long position, byte[] buff);

  public void setBit(long bit);

}
