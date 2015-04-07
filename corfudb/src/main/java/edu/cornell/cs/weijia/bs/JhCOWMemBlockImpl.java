/**
 * 
 */
package edu.cornell.cs.weijia.bs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author sonic
 *
 */
public class JhCOWMemBlockImpl extends JhMemBlock {

	
  /**
   * 
   */
  JhCOWMemBlockImpl() {
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#createSnapshot(edu.cornell.cs.weijia.bs.SnapshotID)
   */
  @Override
  synchronized public void createSS(Object sid) throws BlockSnapshotException {
    SnapshotEntry se = new SnapshotEntry(sid,this.getCurLen());
      snapshots.add(se);
      snapshotMap.put(sid, se);
    
  }

  /**
   * @param sd0 delta for snapshot 0
   * @param sd1 delta for snapshot 1
   * @return new sd0 = sd0 - sd1
   * @throws IOException 
   */
  SnapshotDelta combineSnapshotDelta(SnapshotDelta sd0, SnapshotDelta sd1){
    int len = sd0.length;
    SnapshotDelta nsd = new SnapshotDelta(len);
    TreeMap<Integer,byte[]> newPatchMap = nsd.patchMap;
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4096); // initial 4K

    int pos = 0;
    int pStart = -1;
    while(pos < len){
      Integer p0 = sd0.patchMap.lowerKey(pos+1);
      Integer p1 = sd1.patchMap.lowerKey(pos+1);
      if(p0!=null && (sd0.patchMap.get(p0).length + p0 > pos)){
        //covered by mainlane patch(s0);
        if(pStart<0)pStart=pos;
        byte[]wBuf=sd0.patchMap.get(p0);
        baos.write(wBuf,0,wBuf.length);
        pos += wBuf.length;
      }if(p1!=null&& (sd1.patchMap.get(p1).length + p1 > pos)){
        //covered by auxlane patch(s1)
        if(pStart<0)pStart=pos;
        //get patch (wBuf)
        byte[]wBuf=sd1.patchMap.get(p1);
        //wBuf is restricted by len 
        int wLen = Math.min(len - pos, wBuf.length);
        //and next p0 patch
        Integer np0 = sd0.patchMap.higherKey(pos+1);
        if(np0!=null)wLen = Math.min(wLen,np0 - pos);
        baos.write(wBuf,0,wLen);
        pos += wLen;
      }else{
        //create patch from baos
        if(baos.size()>0){
          newPatchMap.put(pStart,baos.toByteArray());
        }
        //update pos and baos
        baos.reset();
        pStart = -1;
        p0 = sd0.patchMap.higherKey(pos);
        p1 = sd1.patchMap.higherKey(pos);
        if(p0 == null && p1 == null)pos=len;
        else if(p0 == null)pos = p1;
        else if(p1 == null)pos = p0;
        else pos = Math.min(p0, p1);
      }
    }
    if(baos.size()>0){
      newPatchMap.put(pStart,baos.toByteArray());
    }
    
    return nsd;
  }
  
  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#deleteSnapshot(edu.cornell.cs.weijia.bs.SnapshotID)
   */
  @Override
  public void deleteSS(Object sid,boolean bGC) throws BlockSnapshotException {
    // TODO: delete from Bookkeeper
    wl.lock();
	try{
	    // STEP 1: find snapshot
	    SnapshotEntry se = snapshotMap.get(sid);
	    if(se == null)throw new BlockSnapshotException("Snapshot id:"+sid+", is not found");
	    // STEP 2: find previous snapshot
	    ListIterator<SnapshotEntry> litr = snapshots.listIterator(snapshots.indexOf(se));
	    // STEP 3: combine snapshots
	    if(litr.hasPrevious()){
	      SnapshotEntry pse = litr.previous();
	      this.combineSnapshotDelta(pse.delta, se.delta);
	    }
	    // STEP 4: update snapshots data structure
	    snapshots.remove(se);
	    snapshotMap.remove(sid);
	}finally{
		wl.unlock();
	}
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#getSnapshotIDList()
   */
  @Override
  public List<Object> getSnapshotIDList() throws BlockSnapshotException {
	  rl.lock();
	  try{
		  List<Object> l = new LinkedList<Object>();
		  Iterator<SnapshotEntry> itr = this.snapshots.iterator();
		  while(itr.hasNext()){
			  l.add(itr.next().sid);
		  }
		  return l;
	  }finally{
		  rl.unlock();
	  }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#read(long, long, byte[], edu.cornell.cs.weijia.bs.SnapshotID)
   */
  @Override
  public void read(int offset, int length, byte[] buf, Object sid)
      throws BlockSnapshotException {
	  rl.lock();
	  try{
		    //
		    SnapshotEntry ese = snapshotMap.get(sid);
		    if(ese == null)
		      throw new BlockSnapshotException("snapshot:"+sid+" does not exist");
		    // validity checking
		    if(offset <0 || offset + length > ese.delta.length)
		      throw new BlockSnapshotException("offset("+
		        offset+")/length("+length+") is invalid for snapshot:"+sid);
		    // copy from the current version
		    System.arraycopy(bb.array(), offset, buf, 0, length);
		    // patch it
		    ListIterator<SnapshotEntry> litr = snapshots.listIterator(snapshots.size());
		    while(litr.hasPrevious()){
		      SnapshotEntry se = litr.previous();
		      // apply patch se:
		      int start = 0;
		      if(se.delta.patchMap.lowerKey(offset)!=null)start = se.delta.patchMap.lowerKey(offset);
		      SortedMap<Integer,byte[]> tm = se.delta.patchMap.subMap(start, offset+length);
		      if(!tm.isEmpty()){
		        Iterator<Integer> dItr = tm.keySet().iterator();
		        while(dItr.hasNext()){
		          int dS = dItr.next();
		          byte[] patch = tm.get(dS);
		          dS -= offset;
		          /**
		           * [ patch ]
		           * ^dS     ^dS+patch.length
		           * [ buffer ]
		           * ^offset  ^length
		           * 1) [ patch ]
		           *       [ buffer ...
		           * 2) [ patch ]
		           * [ buffer ...
		           */
		          if(dS <= 0 && dS + patch.length > 0)
		            //1) dS <= offset
		              System.arraycopy(patch, -dS, buf, 0, Math.min(length, patch.length+dS));
		          else if (dS > 0)
		            //2) dS > offset
		            System.arraycopy(patch, 0, buf, dS, Math.min(patch.length,length-dS));
		        }
		      }
		      if(se == ese)break;
		    }
	  }finally{
		  rl.unlock();
	  }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.Snapshottable#getCurLen(edu.cornell.cs.weijia.bs.SnapshotID)
   */
  @Override
  public int getLen(Object sid) throws BlockSnapshotException {
    return snapshotMap.get(sid).delta.length;
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#read(long, long, byte[])
   */
  @Override
  public void read(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
	  rl.lock();
	  try{
	    if(offset < 0 || offset >= this.length ||
	        offset + length > this.length || length <= 0 ||
	        buf.length < length)
	      throw new BlockSnapshotException("invalid offset/length:"+offset+"/"+length);
	    System.arraycopy(this.bb.array(), offset, buf, 0, length);
	  }finally{
		  rl.unlock();
	  }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#write(long, long, byte[])
   */
  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#write(int, int, byte[])
   */
  @Override
  public void write_to_block(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
	  wl.lock();
	  try{
		    //STEP1 check validity
		    if( offset < 0 || offset > this.length || (offset + length) > BLOCK_SIZE){
		      throw new BlockSnapshotException("write block error:invalid offset("+offset+
		          ") and length("+length+")");
		    }
		    //STEP2 TODO: write ahead log to Bookkeeper
		    //STEP3 update the snapshot
		    SnapshotEntry se = null;
		    if(this.snapshots.size() > 0)
		      se = this.snapshots.peekLast();
		    if(se != null && se.delta.length > offset){
		      //3.1 SAVE DATA
		      byte [] save = new byte[Math.min(length, se.delta.length - offset)];
		      System.arraycopy(bb.array(), offset, save, 0, save.length);
		      ByteBuffer bbSave = ByteBuffer.wrap(save);
		      bbSave.position(0);
		      int sS = offset; // offset of bbSave in block.
		      int sE = offset + bbSave.capacity();
		      //3.2 UPDATE DELTA
		      /**  
		       *   [ save buffer ]
		       *   ^sS            ^sE
		       *   [ patch buffer ]
		       *   ^pS             ^pE
		       *   
		       */
		      // 1) Patches to be deleted
		      Vector<Integer> delSet = new Vector<Integer>();
		      // 2) get the iterator
		      Integer headKey = se.delta.patchMap.lowerKey(sS);
		      Iterator<Integer> itr = null;
		      if(headKey==null)
		        itr = se.delta.patchMap.keySet().iterator();
		      else
		        itr = se.delta.patchMap.tailMap(headKey).keySet().iterator();
		      // 3) handle
		      while(itr.hasNext()){
		        int pS = itr.next();
		        byte [] dBuf = se.delta.patchMap.get(pS);
		        int pE = pS + dBuf.length;
		        if(pE <= sS)continue; //F: end
		        else if(sS >= pS && sE > pE && pE > sS ){
		          // A: update sOfst and continue
		          //  [ssssssss]
		          // [ppppppp]
		          // = 
		          // [ppppppp][ss]
		          //           ^
		          bbSave.position(pE - sS);
		          // bbSave.flip(); ERROR USAGE of flip
		          bbSave = ByteBuffer.wrap(save, pE - sS, bbSave.remaining());
		          sS = pE;
		        }else if(sS >= pS && sE <= pE){
		          // B: throw save, do nothing
		          //   [SSSSS]
		          //  [PPPPPPPP]
		          // =
		          //  [PPPPPPPP]
		          bbSave=null;
		          break;
		        }else if(sS < pS && sE > pE){
		          // C: combine and substitute
		          // [SSSSSSSSS]
		          //   [PPPPP]
		          // = 
		          // [SSPPPPPSS]
		          //  ^
		          System.arraycopy(dBuf, 0, bbSave.array(), pS - sS, dBuf.length);
		          delSet.add(pS);
		        }else if(sS < pS && sE <= pE && sE > pS){
		          // D: combine and continue;
		          // [SSSSSSS]
		          //   [PPPPPPP]
		          // [SSPPPPPPP]
		          byte [] newSave = new byte[pE - sS];
		          try{
		          System.arraycopy(bbSave.array(), 0, newSave, 0, pS - sS);
		          System.arraycopy(dBuf, 0, newSave, pS-sS, dBuf.length);
		          }catch(ArrayIndexOutOfBoundsException aiobe){
		            System.out.println("sS="+sS+",sE="+sE+",pS="+pS+",pE="+pE);
		            System.out.println("bbSave.array().length="+bbSave.array().length+",newSave.len="+newSave.length+" pS-sS="+(pS-sS));
		          }
		          se.delta.patchMap.put(sS, newSave);
		          delSet.add(pS);
		          bbSave=null;
		          break;
		        }else if(!itr.hasNext() || sE <= pS)//E: break;
		        {
		          se.delta.patchMap.put(pS, bbSave.array());
		          break;
		        }
		      }
		      itr = delSet.iterator();
		      while(itr.hasNext())se.delta.patchMap.remove(itr.next());
		      if(bbSave!=null)
		        se.delta.patchMap.put(sS, bbSave.array());
		    }
		    //STEP4 update block 
		    System.arraycopy(buf, 0, bb.array(), offset, length);
		    this.length = Math.max(this.length, offset + length);
	  }finally{
		  wl.unlock();
	  }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#getCurLen()
   */
  @Override
  public int getCurLen() throws BlockSnapshotException {
	  rl.lock();
	  try{
		  return this.length;
	  }finally{
		  rl.unlock();
	  }
  }

  /* (non-Javadoc)
   * @see edu.cornell.cs.weijia.bs.MemBlock#getBlockSize()
   */
  @Override
  public int getBlockSize() {
    return BLOCK_SIZE;
  }

}
