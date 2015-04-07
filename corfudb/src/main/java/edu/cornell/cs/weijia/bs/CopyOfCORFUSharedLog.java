package edu.cornell.cs.weijia.bs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.abstractions.SharedLog;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.tests.CorfuHello;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CopyOfCORFUSharedLog {
	String masteraddress = "http://localhost:8002/corfu";
	private static final Logger log = LoggerFactory.getLogger(CorfuHello.class);
	final int numthreads=1;
	CorfuDBClient client;
	Sequencer seq;
	WriteOnceAddressSpace woas;
	SharedLog shared_log;
	
	abstract class ParameterType
	{
		String cmd;
		
		abstract byte[] getBytes();
		
		abstract Object getObject(byte[] objb);
		
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
	    		in = new ObjectInputStream(  bis);
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
	   
	    byte[] getBytes()
	    {
	    	byte[] objb;
			  objb= new byte[offset.getBytes().length + length.getBytes().length+ buf.length];
			  System.arraycopy(offset.getBytes(), 0, objb, 0, offset.getBytes().length);
			  System.arraycopy(length.getBytes(), 0, objb, offset.getBytes().length, length.getBytes().length);
			  System.arraycopy(buf,0, objb, offset.getBytes().length+ length.getBytes().length, buf.length);
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
	public CopyOfCORFUSharedLog()
	{
	      client = new CorfuDBClient(masteraddress);
	      seq = new Sequencer(client);
	      woas = new WriteOnceAddressSpace(client);
	      shared_log = new SharedLog(seq, woas);
	      client.startViewManager();
	}
	public boolean writeToLog(String cmd, Object sid)
	{
		if(writeToLog(cmd, new ParameterType1(cmd,sid)))
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
			  
			  cmd_address = shared_log.append(cmd.getBytes());	
			  param_address=shared_log.append(param.getBytes());
			  log.info("Successfully appended "+cmd+" into log position " + cmd_address);
			  log.info("Successfully appended parameter into log position " + param_address);			  
		 /*try 
		  {
			log.info("Reading back entry at address " + cmd_address);
			byte[] cmd_result,param_result;
			cmd_result = shared_log.read(cmd_address);
			param_result=shared_log.read(param_address);			
			log.info("Readback complete, cmd size=" + cmd_result.length);
			log.info("Readback complete, param size=" + param_result.length);
		    String sresult = new String(cmd_result, "UTF-8");		    
		    log.info("Contents were: " + sresult);	
		    //compare param result with the object
		    Object obj=param.getObject(param_result);		    
		    switch (sresult)
		    {
		    case "1":
		    case "2":   	 
		    	break;
		    case "3":
		    	log.info("Parameter value for write "+ ((ParameterType2)obj).length + ((ParameterType2)obj).offset + ((ParameterType2)obj).buf);		    	
		    	break;
		    }
		      if(!sresult.toString().equals(cmd))
		      {		    	  
		    	  return true;
		      }
		      
		} catch (Exception e) {
			
			e.printStackTrace();
		}*/
		return true;
	      
	  }
	
}
