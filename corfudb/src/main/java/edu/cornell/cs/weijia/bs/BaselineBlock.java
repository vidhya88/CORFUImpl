/**
 * 
 */
package edu.cornell.cs.weijia.bs;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author root
 *
 */
public class BaselineBlock extends MemBlock {

  

  byte [] b = new byte[BLOCK_SIZE];
  int len = 0;
  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#createSnapshot(java.lang.Object)
   */
  @Override
  public void createSS(Object sid) throws BlockSnapshotException {
    // do nothing
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#deleteSnapshot(java.lang.Object)
   */
  @Override
  public void deleteSS(Object sid, boolean bGC) throws BlockSnapshotException {
    // do nothing
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#getSnapshotIDList()
   */
  @Override
  public List<Object> getSnapshotIDList() throws BlockSnapshotException {
    return null;
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#read(int, int, byte[], java.lang.Object)
   */
  @Override
  public void read(int offset, int length, byte[] buf, Object sid)
      throws BlockSnapshotException {
    read(offset,length,buf);
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#getLen(java.lang.Object)
   */
  @Override
  public int getLen(Object sid) throws BlockSnapshotException {
    return len;
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#read(int, int, byte[])
   */
  @Override
  public void read(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
    rl.lock();
    try{
      if(offset < 0 || length <=0 || offset + length > len)
        throw new BlockSnapshotException("invalid offset and length");
      System.arraycopy(b, offset, buf, 0, length);
    }finally{
      rl.unlock();
    }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#write(int, int, byte[])
   */
  @Override
  public void write_to_block(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
    
      if(offset < 0 || length <= 0 || offset + length > BLOCK_SIZE || offset > len)
        throw new BlockSnapshotException("invalid offset and length");
      System.arraycopy(buf, 0, b, offset, length);
      len = Math.max(len, offset + length);
    
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#getCurLen()
   */
  @Override
  public int getCurLen() throws BlockSnapshotException {
    return len;
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#getBlockSize()
   */
  @Override
  public int getBlockSize() {
    return BLOCK_SIZE;
  }
}
