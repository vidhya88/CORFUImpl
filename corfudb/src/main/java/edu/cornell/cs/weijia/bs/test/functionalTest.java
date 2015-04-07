package edu.cornell.cs.weijia.bs.test;
import java.util.Random;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.tests.CorfuHello;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cornell.cs.weijia.bs.*;
import edu.cornell.cs.weijia.bs.MemBlock.BlockImplement;

/**
 * WRITE: 0000 0000 0000 0000 0000 0000 0000 0000
 * WRITE: [33]1111 --> exception
 * WRITE: [4]1111 [8]2222 [12]3333
 * READ:  0000 1111 2222 3333 0000 0000 0000 0000
 * @author sonic
 *
 */
public class functionalTest {
  private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);
  String masteraddress = "http://localhost:8003/corfu";
  @BeforeClass
  public static void initClass(){
    
  }
  
  @AfterClass
  public static void cleanClass(){
    
  }
  
  //JhMemBlock b = (JhMemBlock)MemBlock.createMemBlock(MemoryManagement.JAVA, SnapshotMechanism.COW);
  MemBlock b = MemBlock.createMemBlock(BlockImplement.PAGE_COW_HEAP);
  
  @Before
  public void initialize(){
  }
  
 @Test
  public void correctTest(){
    MemBlock pjhp = MemBlock.createMemBlock(BlockImplement.PAGE_COW_HEAP);
    MemBlock njhp = MemBlock.createMemBlock(BlockImplement.JAVA_COW_HEAP);
    //1 Prepare random write();
    final int page_size = 4096;
    final int page_num = 16*1024;
    final int snap_num = 10;
    final int writ_num = 10;//1024;
    Random rand = new Random(System.currentTimeMillis());
    byte [] wbuf = new byte[page_size];
    int count=0;
    try{
      for(int i=0;i<page_num;i++){
        pjhp.write(i*page_size, page_size, wbuf);
        count++;
        njhp.write(i*page_size, page_size, wbuf);
      }
      /*for(int i=0;i<snap_num;i++){
        pjhp.createSnapshot("s"+i);
        njhp.createSnapshot("s"+i);
        for(int l=0;l<writ_num;l++){
          rand.nextBytes(wbuf);
          int loc = rand.nextInt(page_size*page_num - wbuf.length);
          int len = rand.nextInt(page_size);
          if(len > 0){
        	  count++;
            njhp.write(loc, len, wbuf);
            pjhp.write(loc, len, wbuf);
          }
        }
      }*/
      System.out.println("COUNT is " + count);
      //compare
      for(int i=0;i<snap_num;i++){
        for(int pi=0;pi<page_num;pi++){
          byte [] bufn = new byte[page_size];
          byte [] bufp = new byte[page_size];
          njhp.read(pi*page_size, page_size, bufn);
          pjhp.read(pi*page_size, page_size, bufp);
          Assert.assertArrayEquals(bufp,bufn);
        }
      }
    }catch(BlockSnapshotException bse){
      System.out.println(bse);
      bse.printStackTrace();
      Assert.fail(bse.toString());
    }
  }
  
  /*public void Shared_log(String value) throws Exception
  { 
      final int numthreads = 1;
      CorfuDBClient client = new CorfuDBClient(masteraddress);
      Sequencer s = new Sequencer(client);
      WriteOnceAddressSpace woas = new WriteOnceAddressSpace(client);
      SharedLog sl = new SharedLog(s, woas);
      client.startViewManager();
      long address = sl.append(value.getBytes());
      
      log.info("Successfully appended "+value+" into log position " + address);
      log.info("Reading back entry at address " + address);
      byte[] result = sl.read(address);
      log.info("Readback complete, result size=" + result.length);
      String sresult = new String(result, "UTF-8");
      log.info("Contents were: " + sresult);
      if (!sresult.toString().equals("hello world"))
              {
                  log.error("ASSERT Failed: String did not match!");
                  System.exit(1);
              }

      log.info("Successfully completed test!");
  }*/
  
  @Test
  public void functionalTest2() throws Exception{
	    //write data
	    try {
	      CORFUSharedLog cfu=new CORFUSharedLog();    
	      cfu.playback();
	      //Shared_log("00000000000000000000000000000000");
	    } catch (Exception e) {
	      Assert.fail("Throwing Exception:"+e);
	    }
  }
  
  @Test
  public void functionalTest1() throws Exception{
    //write data
    try {
      b.write(0, 32, "10000000000000000000000000000000".getBytes());
      //Shared_log("00000000000000000000000000000000");
    } catch (BlockSnapshotException e) {
      Assert.fail("Throwing Exception:"+e);
    }
    try {
      b.write(33, 4, "1111".getBytes());
      //Shared_log("1111");
      Assert.fail("Not throwing any Exception");
    } catch (BlockSnapshotException e) {
      //do nothing
    }
    try {
      b.write(4, 4, "1111".getBytes());
      b.write(8, 4, "2222".getBytes());
      b.write(12, 4, "3333".getBytes());
      byte []buf=new byte[32];
      b.read(0, 32, buf);
      
      Assert.assertArrayEquals("00001111222233330000000000000000".getBytes(),
              buf);
    } catch (BlockSnapshotException e) {
      Assert.fail("Throwing Exception:"+e);
    }
    
    try {
      b.createSnapshot("sa");
      /**
       * WRITE MORE to
       * 0000 1111 2222 3333 4444 5555 6666 7777
       */
      b.write(16, 4, "4444".getBytes());
      b.write(20, 4, "5555".getBytes());
      b.createSnapshot("sb");
      b.write(24, 4, "6666".getBytes());
      b.write(28, 4, "7777".getBytes());
      byte []buf=new byte[32];
      b.read(0, 32, buf);
      Assert.assertArrayEquals("00001111222233334444555566667777".getBytes(),
          buf);
      b.read(0, 32, buf, "sa");
      Assert.assertArrayEquals("00001111222233330000000000000000".getBytes(),
          buf);
      b.read(0, 32, buf, "sb");
      Assert.assertArrayEquals("00001111222233334444555500000000".getBytes(),
          buf);
      b.createSnapshot("sc");
      b.write(32, 4, "8888".getBytes());
      b.read(0, 32, buf);
      Assert.assertArrayEquals("00001111222233334444555566667777".getBytes(),
          buf);
      buf = new byte[8];
      b.read(28, 8, buf);
      Assert.assertArrayEquals("77778888".getBytes(), buf);
      
      /* DELETE SNAPSHOTS */
      b.deleteSnapshot("sb");
      b.read(16, 8, buf,"sa");
      Assert.assertArrayEquals("00000000".getBytes(), buf);
      b.read(16, 8, buf,"sc");
      Assert.assertArrayEquals("44445555".getBytes(), buf);
      b.deleteSnapshot("sa");
      b.deleteSnapshot("sc");
      b.read(28, 8, buf);
      Assert.assertArrayEquals("77778888".getBytes(), buf);
    } catch (BlockSnapshotException e) {
      Assert.fail("Exception during functional test");
    }
  }
  
  @After
  public void destruct(){
    
  }

  public static void main(String[] args){
    Result result = JUnitCore.runClasses(functionalTest.class);
    for(Failure failure : result.getFailures()) {
      System.out.println(failure.toString());
    }
  }
}
