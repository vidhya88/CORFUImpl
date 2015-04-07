package edu.cornell.cs.weijia.bs;

import java.util.List;

public class JhROWMemBlockImpl extends JhMemBlock {

  public JhROWMemBlockImpl() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void createSS(Object sid) throws BlockSnapshotException {
    // TODO Auto-generated method stub

  }

  @Override
  public void deleteSS(Object sid,boolean bGC) throws BlockSnapshotException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Object> getSnapshotIDList() throws BlockSnapshotException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void read(int offset, int length, byte[] buf, Object sid)
      throws BlockSnapshotException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getLen(Object sid) throws BlockSnapshotException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void read(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(int offset, int length, byte[] buf)
      throws BlockSnapshotException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getCurLen() throws BlockSnapshotException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getBlockSize() {
    // TODO Auto-generated method stub
    return 0;
  }

@Override
public void write_to_block(int offset, int length, byte[] buf)
		throws BlockSnapshotException {
	// TODO Auto-generated method stub
	
}

}
