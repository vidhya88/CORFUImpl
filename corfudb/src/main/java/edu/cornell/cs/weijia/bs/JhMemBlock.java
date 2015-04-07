package edu.cornell.cs.weijia.bs;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author sonic
 * block memory is managed by Java heap
 */
public abstract class JhMemBlock extends MemBlock {
  
  /**
   * @author sonic
   * Store the delta of a block
   */
  class SnapshotDelta{
    /**
     * The block size at this snapshot.
     */
    int length;
    /**
     * 
     */
    TreeMap<Integer,byte[]> patchMap;
    SnapshotDelta(int bLen){
      length = bLen;
      patchMap = new TreeMap<Integer,byte[]>();
    }
  }
  
  class SnapshotEntry{
    /**
     * Snapshot ID
     */
    Object sid;
    SnapshotDelta delta;
    SnapshotEntry(Object sid,int bLen){
      this.sid = sid;
      delta = new SnapshotDelta(bLen);
    };
  }

  
  /**
   * Current block buffer
   */
  ByteBuffer bb = ByteBuffer.wrap(new byte[BLOCK_SIZE]);
  /**
   * Snapshot Queue
   */
  LinkedList<SnapshotEntry> snapshots = new LinkedList<SnapshotEntry>();
  Map<Object,SnapshotEntry> snapshotMap = new HashMap<Object,SnapshotEntry>();
  /**
   * Current block length
   */
  int length = 0;
  
  public JhMemBlock() {
  }
}
