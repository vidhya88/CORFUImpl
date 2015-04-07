package edu.cornell.cs.weijia.bs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.tests.CorfuHello;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.cornell.cs.weijia.bs.MemBlock.BlockImplement;

public class CORFUSharedLog {
	String masteraddress = "http://localhost:8002/corfu";
	private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);
	final int numthreads=1;
	CorfuDBClient client;
	Sequencer seq;
	WriteOnceAddressSpace woas;
	SharedLog shared_log;
	
public enum Command{
		CREATE_SS(1),DELETE_SS(2),WRITE(3),CREATE_MEM_BLOCK(4),DELETE_MEM_BLOCK(5);
		private int numVal;
		Command(int numVal) {
	        this.numVal = numVal;
	    }

	    public int getNumVal() {
	        return numVal;
	    }
	}
	abstract class ParameterType 
	{
		String cmd;		
		abstract byte[] getBytes();		
		abstract  Object getObject(byte[] objb);		
	}

	class ParameterType1 extends ParameterType // for create and delete snapshot
	{
	    Object sid;
	   
	    
	    byte[] getBytes()
	    {
	    	byte[] sid_b=null;
	    	try {
				sid_b=Convert_to_bytearray();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	return sid_b;
	    }
	    
	    public ParameterType1(String pcmd,Object psid)
	    {
	    	sid=psid;
	    	cmd=pcmd;
	    	
	    	
	    }
	    public ParameterType1()
	    {
	    	
	    }
	    public Object getObject(byte[] objb)
	    {
	    	Object obj=null;
	    	  try {
				obj=Convert_bytes_to_object(objb);
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	  return obj;
	    }
	   
	    byte[]  Convert_to_bytearray() throws IOException
	    {	    	
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    ObjectOutput out = null;
	    byte[] sid_b=null;
	    try {
	      out = new ObjectOutputStream(bos);   
	      out.writeObject(sid);
	      sid_b = bos.toByteArray();
	    } finally {
	      try {
	        if (out != null) {
	          out.close();
	        }
	      } catch (IOException ex) {
	        
	      }
	      try {
	        bos.close();
	      } catch (IOException ex) {
	    
	      }
	    }
	    return sid_b;
	}
	    
	    Object Convert_bytes_to_object(byte[] sid_b) throws IOException, ClassNotFoundException
	    {
	    	ByteArrayInputStream bis = new ByteArrayInputStream(sid_b);
	    	ObjectInput in = null;
	    	Object obj;
	    	try {
	    		in = new ObjectInputStream( bis);
	    		obj = in.readObject(); 
	      
	    	} finally {
	    		try {
	    			bis.close();
	    		} catch (IOException ex) {
	        // ignore close exception
	    		}
	    		try {
	    			if (in != null) {
	    				in.close();
	    			}
	    		} catch (IOException ex) {
	    			// ignore close exception
	    		}
	    	}
	    	return obj;
	    }
	}

	class ParameterType2 extends ParameterType //write
	{
	    String offset;
	    String length;
	    byte[] buf; 
	 
	    public ParameterType2(String pcmd,int poffset,int plength,byte[] pbuf)
	    {
	    	cmd=pcmd;
	    	offset=String.valueOf(poffset);
	    	length=String.valueOf(plength);
	    	buf=pbuf;
	    }
	    public ParameterType2()
	    {
	    	cmd="";
	    	offset="0";
	    	length="0";
	    	buf=new byte[100];
	    }
	   
	    byte[] getBytes()
	    {
	    	  byte[] objb;
			  objb= new byte[offset.getBytes().length +"-".getBytes().length+ length.getBytes().length+ "-".getBytes().length+buf.length];
			  System.arraycopy((offset+"-").getBytes(), 0, objb, 0, (offset+"-").getBytes().length);
			  System.arraycopy((length+"-").getBytes(), 0, objb, (offset+"-").getBytes().length, (length+"-").getBytes().length);
			  System.arraycopy(buf,0, objb, (offset+"-").getBytes().length+ (length+"-").getBytes().length, buf.length);
			  return objb;
			  
			
	    }
	    
	    
	    public Object getObject(byte[] objb)
	    {
	    	Object obj=null;
	    	try {
				obj=Convert_bytes_to_object(objb);				
			}catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	return obj;
	    }
	    
	    Object Convert_bytes_to_object(byte[] objb) throws IOException, ClassNotFoundException
	    {	    		    	
	    	ByteArrayInputStream bis = new ByteArrayInputStream(objb);
	    	ObjectInputStream in = null;
	    	Object obj;
	    	try {
	    		in = new ObjectInputStream(bis);
	    		obj = in.readObject(); 	      
	    	} finally {
	    		try {
	    			bis.close();
	    		} catch (IOException ex) {
	        // ignore close exception
	    		}
	    		try {
	    			if (in != null) {
	    				in.close();
	    			}
	    		} catch (IOException ex) {
	    			// ignore close exception
	    		}
	    	}
	    	return obj;
	    }
	    
	}
	
	//for create memblocks
	class ParameterType3 extends ParameterType{
		String mem_block_type;
		String cmd;
		public ParameterType3(String pcmd,String p_mem_block_type)
	    {
	    	mem_block_type=p_mem_block_type;
	    	cmd=pcmd;
	    }
		@Override
		byte[] getBytes() {
			// TODO Auto-generated method stub
			return ("-"+mem_block_type).getBytes();
		}
		@Override
		Object getObject(byte[] objb) {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	public CORFUSharedLog()
	{
	      client = new CorfuDBClient(masteraddress);
	      seq = new Sequencer(client);
	      woas = new WriteOnceAddressSpace(client);
	      shared_log = new SharedLog(seq, woas);
	      client.startViewManager();
	}
	
	public void playback()
	{
		int i=0;
		MemBlock b=null;// = MemBlock.createMemBlock(BlockImplement.PAGE_COW_HEAP);
		MemBlock.restore_mode=true;
		while(true)
		{
			try 
			  {
				log.info("Reading back entry at address " + i);
				byte[] result,cmd_result,param_result;
				result = shared_log.read(i++);
				cmd_result=Arrays.copyOfRange(result, 0, 1);
				param_result=Arrays.copyOfRange(result, 1,result.length);			
				log.info("Readback complete, cmd size=" + cmd_result.length);
				log.info("Readback complete, param size=" + param_result.length);
			    String sresult = new String(cmd_result, "UTF-8");		    
			    log.info("Contents were: " + sresult);	
			    
			    if(sresult.startsWith(String.valueOf(Command.CREATE_MEM_BLOCK.getNumVal()))){
			    	String[] temp=(new String (param_result,"UTF-8")).split("-");	
			    	log.info("CREATE Block CMD "+new String(param_result,"UTF-8"));
			    	b= MemBlock.createMemBlock(BlockImplement.valueOf(temp[1]));
			    }
			    else if(sresult.startsWith(String.valueOf(Command.CREATE_SS.getNumVal())) || sresult.startsWith(String.valueOf(Command.DELETE_SS.getNumVal()))){
			    	ParameterType param=new ParameterType1();
			    	Object sid=param.getObject(param_result);				    	
			    	if(sresult.equals(String.valueOf(Command.CREATE_SS.getNumVal()))){
			    		log.info("CREATE CMD "+new String(param_result,"UTF-8"));
			    		if(b!=null)
			    			b.createSnapshot(sid);
			    	}
			    	else{
			    		log.info("DELETE CMD "+new String(param_result,"UTF-8"));
			    		if(b!= null)
			    			b.deleteSnapshot(sid);
			    	}
			    }
			    else{
			    
			    	String[] temp=(new String (param_result,"UTF-8")).split("-");	
			    	log.info("WRITE COMMAND "+new String (param_result,"UTF-8"));
			    	if(b!=null)
			    		b.write(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]), temp[2].getBytes());
			    }			    	    
			      
			} catch (UnwrittenException e) {
			
				break;
			}
			catch (Exception e){
				
				break;
				
			}
			
				
			
		}
		MemBlock.restore_mode=false;
	}
	
	public boolean writeToLog(String cmd, Object sid)
	{
		if(writeToLog(cmd, new ParameterType1(cmd,sid)))
			return true;
		else
			return false;
	}
	public boolean writeToLog(String cmd, String mem_block_type){
		if(writeToLog(cmd, new ParameterType3(cmd,mem_block_type)))
			return true;
		else
			return false;
	}
	public boolean writeToLog(String cmd,int offset,int length,byte[] buf )
	{
		if(writeToLog(cmd, new ParameterType2(cmd,offset,length,buf)))
			return true;
		else
			return false;
	}
	
	public boolean writeToLog(String cmd,ParameterType param) 
	  { 		
			long cmd_address=0,param_address=0;			
			// create a destination array that is the size of the two arrays
			byte[] destination = new byte[cmd.getBytes().length+param.getBytes().length];	
		   	System.arraycopy(cmd.getBytes(), 0, destination, 0, cmd.getBytes().length); 			
			System.arraycopy(param.getBytes(), 0, destination, cmd.getBytes().length, param.getBytes().length);			
			cmd_address = shared_log.append(destination);				 
			log.info("Successfully appended "+cmd+" into log position " + cmd_address);		 
			return true;
	      
	  }
	
}
