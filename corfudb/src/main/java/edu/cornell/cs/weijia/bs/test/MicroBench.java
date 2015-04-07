package edu.cornell.cs.weijia.bs.test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import edu.cornell.cs.weijia.bs.*;
import edu.cornell.cs.weijia.bs.MemBlock.BlockImplement;

/**
 * MicroBenchmark for 
 * @author sonic
 *
 */
public class MicroBench {
  private static MemBlock memBlock = null;
  
  static private class Reporter implements Runnable {
	  static final int INTERVAL = 10; // record for every 500 milliseconds
	  
	  Reportable workers[];
	  boolean bRun;
	  StringBuffer sb = new StringBuffer();
	  
	  Reporter(Reportable wts[]){
		  this.workers = wts;
		  bRun = true;
	  }
	  
	  @Override
	  public void run(){
		  try{
			  synchronized(memBlock){
				  memBlock.wait();
			  }
			  
			  while(bRun){
				  sb.append(System.currentTimeMillis());
				  for(int i=0;i<workers.length;i++){
					  sb.append(","+workers[i].counter);
				  }
				  sb.append("\n");
				  Thread.sleep(INTERVAL);
			  }
		  }catch(InterruptedException e){
			  System.err.println("unknown interruption");
			  e.printStackTrace();
			  return;
		  }
	  }
	  
	  public void report(){
		  // in sb:
		  // ts,count,count,count,count
		  System.out.println(sb);
		  
		  long tsStart = workers[0].tsStart;
		  long tsEnd = workers[0].tsEnd;
		  for(int i=1;i<workers.length;i++){
			  tsStart = Math.min(tsStart, workers[i].tsStart);
			  tsEnd = Math.max(tsEnd, workers[i].tsEnd);
		  }
		  long tc = 0;
		  for(int i=0;i<workers.length;i++)
			  tc += workers[i].counter;
		  double tb = tc * (double) workers[0].nRWSize;
		  double ttp = tb / (double) (tsEnd - tsStart) / 1000;
		  System.out.println(ttp+",MB/S");
	  }
	  
	  public void stop(){
		  bRun = false;
	  }
  }
  
  /**
   * touch all bytes in the block
   * @throws BlockSnapshotException
   */
  private static void initialize(String bsimpl,boolean bSnapshot) throws BlockSnapshotException{
    if(memBlock!=null)return;
    if(bsimpl.equals("njhp"))
    	memBlock=MemBlock.createMemBlock(BlockImplement.JAVA_COW_HEAP);
    else if(bsimpl.equals("pjhp"))
    	memBlock=MemBlock.createMemBlock(BlockImplement.PAGE_COW_HEAP);
    else if(bsimpl.equals("pjap"))
    	memBlock=MemBlock.createMemBlock(BlockImplement.PAGE_COW_APP);
    else if(bsimpl.equals("base"))
      memBlock=MemBlock.createMemBlock(BlockImplement.BASELINE);
    else if(bsimpl.equals(""))
    	memBlock=MemBlock.createMemBlock(BlockImplement.PAGE_COW_JNI);
    	
    final int chunkSize = 4096;
    byte buf[] = new byte[chunkSize];
    for(int i=0;i<chunkSize;i++)buf[i] = '0';
    for(int j=0;j<JhMemBlock.BLOCK_SIZE/chunkSize;j++)
      memBlock.write(j*chunkSize, chunkSize, buf);
    if(bSnapshot){
      Snapshottable s =(Snapshottable)memBlock;
      s.createSnapshot(0);
    }
  }
  
  /**
 * @author root
 *
 */
  static abstract private class Reportable{
    int nRWSize;
    int nRWCount;
    int counter = 0;
    long tsStart,tsEnd;
  }

  
  /**
 * @author ws393
 *
 */
  static private class WriteThread extends Reportable implements Runnable{
//    int nRWSize;
//    int nRWCount;
//    int counter = 0;
//    long tsStart,tsEnd;
    Semaphore sema = null;
    
    WriteThread(int pWriteSize, int pWriteCount, Semaphore pSema){
      this.nRWSize = pWriteSize;
      this.nRWCount = pWriteCount;
      this.sema = pSema;
    }
    
    @Override
    public void run() {
      // prepare the write buffer
      byte wbuf[] = new byte[nRWSize];
      byte bSig = (byte)Thread.currentThread().getId();
      for(int i=0;i<nRWSize;i++)wbuf[i] = bSig;

      int offsets[] = new int[nRWCount];
      Random rand = new Random(System.currentTimeMillis());
      for(int i=0;i<nRWCount;i++){
    	  offsets[i] = rand.nextInt(memBlock.getBlockSize() - nRWSize);
      }
      
      sema.release();
      // wait to begin
      try {
        synchronized(memBlock){
          memBlock.wait();
        }
      } catch (InterruptedException e1) {
        System.err.println("unknown interruption");
        e1.printStackTrace();
        return;
      }
      //////////////////////////////////////////////////
      // performan the write operation
      tsStart = System.currentTimeMillis();
      // perform write
      while(true){
        try {
          memBlock.write(offsets[counter], nRWSize, wbuf);
          counter++;
          if(counter>=nRWCount){
            break;
          }
        } catch (BlockSnapshotException e) {
        	System.out.println(e);
        	e.printStackTrace();
        }
      }
      tsEnd = System.currentTimeMillis();
      System.out.println("WriteThread-"+bSig+" is done.");
    }
  }
  
  static private class ReadThread extends Reportable implements Runnable{
	Semaphore sema = null;
	Object snapshotId = null;
	
	ReadThread(int pReadSize, int pReadCount, Semaphore pSema){
      this.nRWSize = pReadSize;
	  this.nRWCount = pReadCount;
	  this.sema = pSema;
	}
	
	ReadThread(int pReadSize, int pReadCount, Object pSnapshot, Semaphore pSema){
		this(pReadSize,pReadCount,pSema);
		this.snapshotId = pSnapshot;
	}
	
	@Override
	public void run() {
		// Prepare the location to be read
		int offsets[] = new int[nRWCount];
		Random rand = new Random(System.currentTimeMillis());
		for(int i=0;i<nRWCount;i++)
		  offsets[i] = rand.nextInt(memBlock.getBlockSize()-nRWSize);
		byte[] rbuf = new byte[nRWSize];
		sema.release();
		// wait for signal
		try {
			synchronized(memBlock){
				memBlock.wait();
			}
		} catch (InterruptedException e1) {
	          System.err.println("unknown interruption");
	          e1.printStackTrace();
	          return;
        }		
		// perform read
		tsStart = System.currentTimeMillis();
		for(;counter<nRWCount;counter++){
	        try {
	        	if(this.snapshotId!=null)
		            memBlock.read(offsets[counter], nRWSize, rbuf, this.snapshotId);
	        	else
	        		memBlock.read(offsets[counter], nRWSize, rbuf);
	          } catch (BlockSnapshotException e) {
	          	System.out.println(e);
	          	e.printStackTrace();
	          }
		}
		tsEnd = System.currentTimeMillis();
	  System.out.println("ReadThread-"+Thread.currentThread().getId()+" is done.");
	}
  }

  static void performRead(int pReadSize, int pReadCount, int pReadThread, int snapshotDepth)
  throws BlockSnapshotException,InterruptedException
  {
	  
	  for(int s=-1;s<snapshotDepth;s++){
	    System.out.println("========Snapshot:s"+s+"========");
	    final Semaphore sema = new Semaphore(0);
	    Thread threads[] = new Thread[pReadThread];
	    ReadThread rts[] = new ReadThread[pReadThread];
	    Reporter reporter = new Reporter(rts);
	    Thread report_thread = new Thread(reporter);
	    report_thread.start();

	    for(int i=0;i<pReadThread;i++)
	    {
	      if(s==-1)
	        rts[i] = new ReadThread(pReadSize,pReadCount,sema);
	      else
	        rts[i] = new ReadThread(pReadSize,pReadCount,"s"+s,sema);
	      threads[i] = new Thread(rts[i]);
	      threads[i].start();
	    }
	    sema.acquire(pReadThread);
	    Thread.sleep(5000);
	    synchronized(memBlock){
	        memBlock.notifyAll();
	    }
	    
	    for(int i=0;i<pReadThread;i++)threads[i].join();
	    
	    reporter.stop();
	    report_thread.join();
	    reporter.report();
	  }
  }
  
  /**
   * Test performance of snapshot creation/deletion
   * @param pWriteSize - write size
   * @param pWriteCount - write count
   * @param pSnapshotDepth - depth of snapshot
   * @param pCount - how many creation/deletion we are test?
   * @param pLazy - using lazy reclaim
   * @throws InterruptedException 
   * @throws BlockSnapshotException 
   */
  static void performSnap(int pWriteSize, int pWriteCount, int pSnapshotDepth, int pCount, boolean pLazy) throws InterruptedException, BlockSnapshotException{
    final Semaphore sema = new Semaphore(0);
    //1 - create snapshot depth
    for(int i=0;i<pSnapshotDepth;i++){
      WriteThread wt = new WriteThread(pWriteSize,pWriteCount,sema);
      Thread t = new Thread(wt);
      t.start();
      sema.acquire(1);
      Thread.sleep(2000);
      synchronized(memBlock){
          memBlock.notifyAll();
      }
      t.join();
      memBlock.createSnapshot(i);
    }
    //2 - delete and create snapshot
    Random rand = new Random(System.currentTimeMillis());
    for(int i=0;i<pCount;i++){
      List<Object> slist = memBlock.getSnapshotIDList();
      Integer s = (Integer)slist.get(rand.nextInt(slist.size()));
      System.out.println("delete " + s + " from" + slist);
      long sts,ets;
      //delete
      sts = System.nanoTime();
      if(memBlock instanceof PageMemBlock && !pLazy)
        ((PageMemBlock)memBlock).deleteSnapshot(s, true);
      else
        memBlock.deleteSnapshot(s);
      ets = System.nanoTime();
      System.out.println( "deletion time (ns):" + (ets-sts) );
      //write
      {
        WriteThread wt = new WriteThread(pWriteSize,pWriteCount,sema);
        Thread t = new Thread(wt);
        t.start();
        sema.acquire(1);
        Thread.sleep(2000);
        synchronized(memBlock){
            memBlock.notifyAll();
        }
        t.join();
      }
      //create
      sts = System.nanoTime();
      memBlock.createSnapshot(pSnapshotDepth+i+1);
      ets = System.nanoTime();
      System.out.println( "creation time (ns):" + (ets-sts) );
    }
    //snapshot garbage collection
    if(memBlock instanceof PageMemBlock){
      long sts,ets;
      sts = System.nanoTime();
      ((PageMemBlock)memBlock).doGC();
      ets = System.nanoTime();
      System.out.println( "GC time (ns):" + (ets-sts) );
    }
  }
  
  static void performWrite(int pWriteSize, int pWriteCount, int pWriteThread, int pSnapshotDepth, boolean bReport)
  throws BlockSnapshotException,InterruptedException
  {
	  final Semaphore sema = new Semaphore(0);
	  
	  for(int s=0;s<pSnapshotDepth;s++){
		  memBlock.createSnapshot("s"+s);
		  if(bReport)
			  System.out.println("========Snapshot:s"+s+"=========");
		  // create threads
		  Thread threads[] = new Thread[pWriteThread];
		  WriteThread wts[] = new WriteThread[pWriteThread];
		  Reporter reporter = new Reporter(wts);
		  Thread report_thread = new Thread(reporter);
		  report_thread.start();
		  
		  for(int i=0;i<pWriteThread;i++)
		  {
		      wts[i] = new WriteThread(pWriteSize,pWriteCount,sema);
		      threads[i] = new Thread(wts[i]);
		      threads[i].start();
		  }
		  sema.acquire(pWriteThread);
		  Thread.sleep(5000);
		  synchronized(memBlock){
		      memBlock.notifyAll();
		  }
		  
		  for(int i=0;i<pWriteThread;i++)threads[i].join();
		  
		  reporter.stop();
		  report_thread.join();
		  if(bReport){
		    reporter.report();
		    ////////////////////////
		    // print memory usage here
		    System.gc();
		    Runtime rt = Runtime.getRuntime();
		    long usedMB = (rt.totalMemory() - rt.freeMemory())/1024/1024;
  	    System.out.println("Mem usage,"+usedMB+",MB");
		  }
	  }
  }

  //Command line options
  static Options ops;
  static final String ARG_TEST = "t";
  static final String ARG_HELP = "h";
  static final String ARG_WRITE_SIZE = "ws";
  static final String ARG_WRITE_COUNT = "wc";
  static final String ARG_READ_SIZE = "rs";
  static final String ARG_READ_COUNT = "rc";
  static final String ARG_BLOCK_IMPL = "bi";
  static final String ARG_SNAPSHOT_DEPTH = "sd";
  static final String ARG_SNAPSHOT_COUNT = "sc";
//  static final String ARG_SNAPSHOT_ID = "si";
  static final String ARG_SNAPSHOT_DELETION_POLICY = "dp";
  static final String ARG_NUM_THREAD = "nt";
  static{
    ops = new Options();
    ops.addOption(OptionBuilder.withArgName("testType")
        .hasArg()
        .withDescription("test type[write|read|snap]")
        .withLongOpt("test")
        .create(ARG_TEST));
    ops.addOption(OptionBuilder.withDescription("print this message")
        .withLongOpt("help")
        .create(ARG_HELP));
    ops.addOption(OptionBuilder.withArgName("writeSize")
        .hasArg()
        .withDescription("size for each write operation")
        .withLongOpt("writeSize")
        .create(ARG_WRITE_SIZE));
    ops.addOption(OptionBuilder.withArgName("writeCount")
        .hasArg()
        .withDescription("numbers of write")
        .withLongOpt("writeCount")
        .create(ARG_WRITE_COUNT));
    ops.addOption(OptionBuilder.withArgName("readSize")
        .hasArg()
        .withDescription("size for each read operation")
        .withLongOpt("readSize")
        .create(ARG_READ_SIZE));
    ops.addOption(OptionBuilder.withArgName("readCount")
        .hasArg()
        .withDescription("numbers of read")
        .withLongOpt("readCount")
        .create(ARG_READ_COUNT));
    ops.addOption(OptionBuilder.withArgName("block implementation")
        .hasArg()
        .withDescription("block implementation:[njhp|pjhp|pjap|pjni]")
        .withLongOpt("blockImpl")
        .create(ARG_BLOCK_IMPL));
    ops.addOption(OptionBuilder.withArgName("snapshot depth")
        .hasArg()
        .withDescription("depth of snapshot")
        .withLongOpt("snapshotDepth")
        .create(ARG_SNAPSHOT_DEPTH));
    ops.addOption(OptionBuilder.withArgName("snapshot count")
        .hasArg()
        .withDescription("how many creation/deletion snapshot")
        .withLongOpt("snapshotCount")
        .create(ARG_SNAPSHOT_COUNT));
//    ops.addOption(OptionBuilder.withArgName("snapshot id")
//        .hasArg()
//        .withDescription("snapshot id")
//        .withLongOpt("snapshotId")
//        .create(ARG_SNAPSHOT_ID));
    ops.addOption(OptionBuilder.withArgName("snapshot deletion policy")
        .hasArg()
        .withDescription("snapshot deletion policy:[eager|lazy]")
        .withLongOpt("snapshotDeletionPolicy")
        .create(ARG_SNAPSHOT_DELETION_POLICY));
    ops.addOption(OptionBuilder.withArgName("thread number")
        .hasArg()
        .withDescription("number of threads")
        .withLongOpt("numThread")
        .create(ARG_NUM_THREAD));
  }
  
  static void printHelp(){
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(MicroBench.class.getName(), ops);
  }

  public static void main(String[] args) throws InterruptedException, BlockSnapshotException {
    
    Parser parser = new BasicParser();
    try{
      CommandLine cl = parser.parse(ops,args,true);
      if(!cl.hasOption(ARG_TEST)){
        printHelp();
        return;
      }else{
        String tType = cl.getOptionValue(ARG_TEST);
        if(tType.equals("write")){
          String bsimpl = cl.getOptionValue(ARG_BLOCK_IMPL);
          int WS = Integer.parseInt(cl.getOptionValue(ARG_WRITE_SIZE));
          int WC = Integer.parseInt(cl.getOptionValue(ARG_WRITE_COUNT));
          int NT = Integer.parseInt(cl.getOptionValue(ARG_NUM_THREAD));
          int SD = Integer.parseInt(cl.getOptionValue(ARG_SNAPSHOT_DEPTH));
          initialize(bsimpl,false);
          performWrite(WS,WC,NT,SD,true);
        }else if(tType.equals("read")){
          String bsimpl = cl.getOptionValue(ARG_BLOCK_IMPL);
          int WS = Integer.parseInt(cl.getOptionValue(ARG_WRITE_SIZE));
          int WC = Integer.parseInt(cl.getOptionValue(ARG_WRITE_COUNT));
          int NT = Integer.parseInt(cl.getOptionValue(ARG_NUM_THREAD));
          int SD = Integer.parseInt(cl.getOptionValue(ARG_SNAPSHOT_DEPTH));
          int RS = Integer.parseInt(cl.getOptionValue(ARG_READ_SIZE));
          int RC = Integer.parseInt(cl.getOptionValue(ARG_READ_COUNT));
//          String SI = cl.getOptionValue(ARG_SNAPSHOT_ID);
          initialize(bsimpl,false);
          performWrite(WS,WC,1,SD,false);
          performRead(RS,RC,NT,SD);
        }else if(tType.equals("snap")){
          String bsimpl = cl.getOptionValue(ARG_BLOCK_IMPL);
          int WS = Integer.parseInt(cl.getOptionValue(ARG_WRITE_SIZE));
          int WC = Integer.parseInt(cl.getOptionValue(ARG_WRITE_COUNT));
          int SD = Integer.parseInt(cl.getOptionValue(ARG_SNAPSHOT_DEPTH));
          int SC = Integer.parseInt(cl.getOptionValue(ARG_SNAPSHOT_COUNT));
          boolean bSDPLazy = true;
          if(cl.hasOption(ARG_SNAPSHOT_DELETION_POLICY)){
            bSDPLazy = (cl.getOptionValue(ARG_SNAPSHOT_DELETION_POLICY).equals("lazy"));
          }
//          String SI = cl.getOptionValue(ARG_SNAPSHOT_ID);
          initialize(bsimpl,false);
          performSnap(WS, WC, SD, SC, bSDPLazy);
        }
      }
    }catch(ParseException pe){
      System.err.println(pe);
      printHelp();
    }
  }
}
