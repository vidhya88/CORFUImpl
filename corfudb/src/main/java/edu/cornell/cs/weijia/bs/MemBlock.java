package edu.cornell.cs.weijia.bs;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import edu.cornell.cs.weijia.bs.CORFUSharedLog.Command;
import edu.cornell.cs.weijia.bs.PageMemBlock.GC_TYPE;

public abstract class MemBlock implements Snapshottable {

  public final static int BLOCK_SIZE = 0x4000000;
  static CORFUSharedLog shared_log;
  ReadWriteLock rwl = new ReentrantReadWriteLock();
  Lock rl = rwl.readLock();
  Lock wl = rwl.writeLock();
  public static boolean restore_mode=false;
  /**
   * Constructor
   */
  public MemBlock() {
  }

  public abstract void read(int offset, int length, byte[]buf)throws BlockSnapshotException;
  public abstract void write_to_block(int offset, int length, byte[]buf)throws BlockSnapshotException;
  public abstract void createSS(Object sid) throws BlockSnapshotException;
  public abstract void deleteSS(Object sid, boolean bGC) throws BlockSnapshotException;
  public abstract int getCurLen()throws BlockSnapshotException;
  public abstract int getBlockSize();
  
  /**
   * @author sonic
   * COW: Copy-On-Write
   * ROW: Redirect-On-Write
   */
  public enum BlockImplement{
	  JAVA_COW_HEAP,
	  PAGE_COW_HEAP,
	  PAGE_COW_APP,
	  PAGE_COW_JNI,
	  BASELINE,};
 
	  
	  public  void write(int offset, int length, byte[]buf)throws BlockSnapshotException{
		  wl.lock();
			try{
				if(!restore_mode)
					shared_log.writeToLog(String.valueOf(Command.WRITE.getNumVal()),offset,length,buf);
				write_to_block(offset,length,buf);
			}finally{
				wl.unlock();
			}
	  }	  
	  
  /**
   * @param mm memory management 
   * @param sm snapshot mechanism
   * @return
   */
  static public MemBlock createMemBlock(BlockImplement bi){
    //by default we are using JavaHeapMemBlock
    MemBlock mb = null;
    shared_log=new CORFUSharedLog();
    if(!restore_mode)
    	shared_log.writeToLog(String.valueOf(Command.CREATE_MEM_BLOCK.getNumVal()), bi.toString());
    switch(bi){
    case JAVA_COW_HEAP:
    	mb = new JhCOWMemBlockImpl();
    	break;
    case PAGE_COW_HEAP:
    	mb = new PageMemBlock(GC_TYPE.JAVA);
    	break;
    case PAGE_COW_APP:
    	mb = new PageMemBlock(GC_TYPE.APP);
    	break;
    case PAGE_COW_JNI:
    	mb = new PageMemBlock(GC_TYPE.JNI);
    	break;
    case BASELINE:
      mb = new BaselineBlock();
      break;
    default:
    	break;
    }
    return mb;
  }
  
  public void createSnapshot(Object sid)
  {
	//acquire write lock
	 // write to shared log
	  // call appropriate child class function
	  //release lock
	  try{
		  wl.lock();
		  //write to CORFU Log
		  if(!restore_mode)
			  shared_log.writeToLog(String.valueOf(Command.CREATE_SS.getNumVal()),sid);
		  createSS(sid);
	  }
		catch(Exception exp)
		{
			
		}
		finally{
			wl.unlock();
		}
  }
  
  public void deleteSnapshot(Object sid)
  {
	  wl.lock();
		try{
			//write to shared log
			if(!restore_mode)
				shared_log.writeToLog(String.valueOf(Command.DELETE_SS.getNumVal()),sid);
			deleteSS(sid,false);
		}
		catch(BlockSnapshotException e)
		{
			
		}
		catch(Exception exp)
		{
			
		}
		finally{
			wl.unlock();
		}
  }
  
  
  public void deleteSnapshot(Object sid, boolean bGC)
  {
	  wl.lock();
		try{
			//write to shared log
			if(!restore_mode)
				shared_log.writeToLog(String.valueOf(Command.DELETE_SS.getNumVal()),sid);
			deleteSS(sid,bGC);
		}
		catch(BlockSnapshotException e)
		{
			
		}
		catch(Exception exp)
		{
			
		}
		finally{
			wl.unlock();
		}
  }
  
  
}
