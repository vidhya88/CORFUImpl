package edu.cornell.cs.weijia.bs.test;

import java.util.*;

public class RawWritePerformance {
  final static int BLOCK_SIZE = 4096;
  final static int NWRITE = (1<<20);
  final static int CPU_CACHE_SIZE = 6*1024*1024;
  final static int BUFFER_SIZE = CPU_CACHE_SIZE*10;

  static double testWriteThroughput(int bsize){
	  
    byte [] from = new byte[bsize];
    byte [] to = new byte [BUFFER_SIZE];
    
    Random rand = new Random(System.currentTimeMillis());
    rand.nextBytes(from);
    long nMax = Math.min((long)NWRITE, 2048l*1024*1024/bsize);
    long ts = System.currentTimeMillis();
    for(int i=0;i<nMax;i++)
    {
    	int offset = ((i*bsize) % (BUFFER_SIZE - bsize));
   		System.arraycopy(from, 0 , to, offset , bsize);
    }
    long time = System.currentTimeMillis() - ts;
    double volume = nMax*(double)bsize;
    return volume/time/1000;
  }
  
  public static void main(String args[]){
	if(args.length!=2){
		System.err.println("Usage:"+RawWritePerformance.class.getName()+" <size_order> <size_order>");
		return;
	}
    for(int order=Integer.parseInt(args[0]);order<=Integer.parseInt(args[1]);order++){
      System.out.println("block size:"+(1<<order));
      System.out.println(testWriteThroughput(1<<order));
      System.out.println(testWriteThroughput(1<<order));
      System.out.println(testWriteThroughput(1<<order));
      System.out.println(testWriteThroughput(1<<order));
      System.out.println(testWriteThroughput(1<<order));
    }
  }
}
