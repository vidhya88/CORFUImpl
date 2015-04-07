/**
 * 
 */
package edu.cornell.cs.weijia.bs;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author root
 *
 */
public class PageMemBlock extends MemBlock {
	final static int PAGE_SIZE = 0x1000; // 4K page
//	final static int PAGE_SIZE = 0x800; // 2K page
//	final static int PAGE_SIZE = 0x400; // 1K page
//	final static int PAGE_SIZE = 0x200; // 512B page
//	final static int PAGE_SIZE = 0x100; // 256B page
//	final static int PAGE_SIZE = 0x80; // 128B page
//	final static int PAGE_SIZE = 0x40; // 64B page
	final static int MAX_ACTIVE_SNAPSHOT = (0x400 - 1); // only 1024 active snapshots allowed
	
	public enum GC_TYPE{JAVA,APP,JNI};
	
	private class SnapshotEntry{
		Object sid; // snapshot id
		ByteBuffer[] pgtbl; // page table
		int length; // block length
		boolean tombStone; // Is this a dead snapshot?
	}

	//final ReadWriteLock rwl = new ReentrantReadWriteLock();
	//final Lock rl = rwl.readLock();
	//final Lock wl = rwl.writeLock();
	Map<Object, Integer> sidMap; //sid --> index
	final GC_TYPE gctype;
	//CORFUSharedLog shared_log;
	
	SnapshotEntry [] scq; // snapshot circular queue
	int head = 0; // head
	int tail = 0; // tail
	int cnt = 0; // how many active snapshot we have now.
	boolean hasTombstone = false;
	// non-reentrant tools.
	private int next(int pos){
		return (pos + 1)%(MAX_ACTIVE_SNAPSHOT + 1);
	}
	private int prev(int pos){
		return (pos + MAX_ACTIVE_SNAPSHOT)%(MAX_ACTIVE_SNAPSHOT + 1);
	}
	private int getNumSnapshot(){
		return (tail - head + MAX_ACTIVE_SNAPSHOT + 1) % (MAX_ACTIVE_SNAPSHOT + 1);
	}
	private void initialize(int pos){
		if(scq[pos]==null)scq[pos] = new SnapshotEntry();
		scq[pos].sid = null;
		scq[pos].length = 0;
		scq[pos].tombStone = false;
		scq[pos].pgtbl = null;
	}
	
	
	private void gc(){
		if(!hasTombstone)return; //no tombstone, skip.
		// STEP 1: clean pages
		for(int pos = head; next(pos) !=tail; pos = next(pos) ){
			if(!scq[pos].tombStone || scq[pos].pgtbl==null) continue;
			for(int pgidx = 0; pgidx < BLOCK_SIZE/PAGE_SIZE; pgidx++){
				if(scq[pos].pgtbl[pgidx]==null)continue;
				int sidx = pos;
				for(;sidx != tail; sidx = next(sidx)){
					if(scq[sidx].pgtbl[pgidx]!=null)break;
				}
				// we didn't find following updates on this page,
				if(sidx==tail)
					scq[next(pos)].pgtbl[pgidx] = scq[pos].pgtbl[pgidx];
				else
					reclaimPage(scq[pos].pgtbl[pgidx]);
			}
		}
		// STEP 2: compact the SCQ
		SnapshotEntry [] nscq = new SnapshotEntry[MAX_ACTIVE_SNAPSHOT];
		Map<Object,Integer> nSidMap = new HashMap<Object,Integer>();
		int l = 0;
		for(int i = head;i!=tail;i=next(i)){
			if(scq[i].tombStone)continue;
			nscq[l] = new SnapshotEntry();
			nscq[l].length = scq[i].length;
			nscq[l].pgtbl = scq[i].pgtbl;
			nscq[l].sid = scq[i].sid;
			nscq[l].tombStone = false;
			nSidMap.put(nscq[l].sid, l);
			l++;
		}
		// SETP 3 substitute SCQ
		this.scq = nscq;
		this.head = 0;
		this.tail = l;
	}
	
	
	LinkedList<ByteBuffer> pagePool;
	private ByteBuffer allocPage(){
		ByteBuffer bb = null;
		switch(this.gctype){
		case JAVA:
			bb = ByteBuffer.allocate(PAGE_SIZE);
			break;
		case APP:
			synchronized(pagePool){
				if(!pagePool.isEmpty())
					bb = pagePool.pop();
			}
			if(bb==null)
				bb = ByteBuffer.allocate(PAGE_SIZE);
			break;
		case JNI:
		default:
			return null;
		}
		return bb;
	}
	private void reclaimPage(ByteBuffer bb){
		if(this.gctype == GC_TYPE.APP){
			synchronized(pagePool){
				pagePool.add(bb);
			}
		}		
	}
	/**
	 * Constructor
	 */
	PageMemBlock(GC_TYPE gctype){
		super();
		sidMap = new HashMap<Object,Integer>();
		scq = new SnapshotEntry[MAX_ACTIVE_SNAPSHOT + 1];
		//prepare current snapshot
		tail = 1;
		initialize(head);
		cnt = 1;
		//garbage collector
		this.gctype = gctype;
		if(gctype==GC_TYPE.APP)
			pagePool = new LinkedList<ByteBuffer>();
		//shared_log=new CORFUSharedLog();
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.Snapshottable#createSnapshot(java.lang.Object)
	 */
	@Override
	public void createSS(Object sid) throws BlockSnapshotException  {
		
		if(cnt >= MAX_ACTIVE_SNAPSHOT)
			throw new BlockSnapshotException("fail to create snapshot because of maximum number of snapshot:" + cnt);			
			// STEP 1: garbage collection for deleted snapshot
			if(getNumSnapshot() >= MAX_ACTIVE_SNAPSHOT)
				gc();
			// SETP 2: create a snapshot
			//// prepare a new snapshot entry
			if(scq[tail]==null)scq[tail] = new SnapshotEntry();
			scq[tail].sid = null;
			scq[tail].length = scq[prev(tail)].length;
			scq[tail].pgtbl = null;
			scq[tail].tombStone = false;
			//// put current snapshot entry to map
			scq[prev(tail)].sid = sid;
			sidMap.put(sid, prev(tail));
			//// grow to new snapshot entry
			tail = next(tail);
			cnt ++;
		  
		
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.Snapshottable#deleteSnapshot(java.lang.Object)
	 */
	
	
	@Override
	public void deleteSS(Object sid, boolean bGC) {
		
			
			int pos = sidMap.get(sid);
			scq[pos].tombStone = true;
			this.hasTombstone = true;
			cnt --;
			if(bGC)
			  this.gc();
			
			
		
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.Snapshottable#getSnapshotIDList()
	 */
	@Override
	public List<Object> getSnapshotIDList() throws BlockSnapshotException {
		rl.lock();
		try{
			List<Object> l = new LinkedList<Object>();
			int pos = head;
			while(pos!=prev(tail)){
			  if(!scq[pos].tombStone)
			    l.add(scq[pos].sid);
				pos = next(pos);
			}
			return l;
		}finally{
			rl.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.Snapshottable#read(int, int, byte[], java.lang.Object)
	 */
	@Override
	public void read(int offset, int length, byte[] buf, Object sid)
			throws BlockSnapshotException {
		if(offset < 0 || offset + length > BLOCK_SIZE )
			throw new BlockSnapshotException("Invalid offset and length:" + offset + "," + length);
		rl.lock();
		try{
			int pos = prev(tail);
			if(sid != null){
				if(sidMap.get(sid) == null)
					throw new BlockSnapshotException("Snapshot ID is not found:"+sid);
				pos = sidMap.get(sid);
			}
			if(offset + length > scq[pos].length)
				throw new BlockSnapshotException("Read through end of block:" + (offset + length) + "," + scq.length);
			
			int page_off = offset % PAGE_SIZE;
			int page_idx = offset / PAGE_SIZE;
			int read_len = length;
			while(read_len > 0){
				int tpos = pos;
				while(scq[tpos].pgtbl[page_idx] == null)tpos = prev(tpos);
				int read_n = Math.min(read_len, PAGE_SIZE - page_off);
				System.arraycopy(scq[tpos].pgtbl[page_idx].array(), page_off, buf, length - read_len, read_n);
				//update read_len, offset, index
				read_len -= read_n;
				page_idx ++;
				page_off = 0;
			}
		}finally{
			rl.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.Snapshottable#getCurLen(java.lang.Object)
	 */
	@Override
	public int getLen(Object sid) throws BlockSnapshotException {
		rl.lock();
		try{
			if(sidMap.get(sid) == null)
				throw new BlockSnapshotException("Snapshot ID is not found:"+sid);
			return scq[sidMap.get(sid)].length;
		}finally{
			rl.unlock();
		}
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.MemBlock#read(int, int, byte[])
	 */
	@Override
	public void read(int offset, int length, byte[] buf)
			throws BlockSnapshotException {
		read(offset,length,buf,null);
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.MemBlock#write(int, int, byte[])
	 */
	@Override
	public void write_to_block(int offset, int length, byte[] buf)
			throws BlockSnapshotException {
		
			if(offset > getCurLen() ||
					offset + length > BLOCK_SIZE)
				throw new BlockSnapshotException("Invalid offset and length:" + offset + "," + length);
			int page_off = offset % PAGE_SIZE;
			int page_idx = offset / PAGE_SIZE;
			int writ_len = length;
			int cur = prev(tail);
			while(writ_len > 0){
				if(scq[cur].pgtbl == null)scq[cur].pgtbl = new ByteBuffer[BLOCK_SIZE / PAGE_SIZE];
				if(scq[cur].pgtbl[page_idx] == null){
					scq[cur].pgtbl[page_idx] = allocPage();
					if(scq[cur].length > page_idx * PAGE_SIZE)
					for(int pcur = prev(cur);pcur!=prev(head);pcur=prev(pcur)){
						if(scq[pcur].pgtbl!=null && scq[pcur].pgtbl[page_idx]!=null){
							//TODO: maybe use a reference to the page.
							System.arraycopy(scq[pcur].pgtbl[page_idx].array(), 0, 
									scq[cur].pgtbl[page_idx].array(), 0, PAGE_SIZE);
							break;
						}
					}
				}
				int writ_num = Math.min(PAGE_SIZE - page_off, writ_len);
				System.arraycopy(buf, length - writ_len, scq[cur].pgtbl[page_idx].array(), page_off, writ_num);
				//udpate writ_len, page_off, page_idx
				writ_len -= writ_num;
				page_idx ++;
				page_off = 0;
			}
			scq[cur].length = Math.max(scq[cur].length, offset + length);
			
		
	}

	/* (non-Javadoc)
	 * @see edu.cornell.cs.weijia.bs.MemBlock#getCurLen()
	 */
	@Override
	public int getCurLen() throws BlockSnapshotException {
		rl.lock();
		try{
			return scq[prev(tail)].length;
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
	
	public void doGC(){
	  wl.lock();
	  try{
	    gc();
	  }finally{
	    wl.unlock();
	  }
	}
}
