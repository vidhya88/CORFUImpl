/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// @author Dahlia Malkhi
//
// implement a cyclic log store: logically infinite log sequence mapped onto a UNICAPACITY array of fixed-entrys
package org.corfudb.infrastructure;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.*;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import org.corfudb.infrastructure.thrift.SimpleLogUnitService;
import org.corfudb.infrastructure.thrift.SimpleLogUnitConfigService;
import org.corfudb.infrastructure.thrift.SimpleLogUnitWrap;
import org.corfudb.infrastructure.thrift.UnitServerHdr;
import org.corfudb.infrastructure.thrift.ExtntInfo;
import org.corfudb.infrastructure.thrift.ExtntWrap;
import org.corfudb.infrastructure.thrift.ExtntMarkType;
import org.corfudb.infrastructure.thrift.ErrorCode;
import org.corfudb.infrastructure.thrift.UnitServerHdr;

public class SimpleLogUnitServer implements SimpleLogUnitService.Iface, ICorfuDBServer {
	private Logger log = LoggerFactory.getLogger(SimpleLogUnitServer.class);

    List<Integer> masterIncarnation = null;
    protected int UNITCAPACITY = 100000; // capacity in PAGESIZE units, i.e. UNITCAPACITY*PAGESIZE bytes
    protected int PORT=-1;	// REQUIRED: port number this unit listens on
    protected String DRIVENAME = null; // where to persist data (unless rammode is on)
    protected boolean RAMMODE = false; // command line switch: work in memory (no data persistence)
    protected boolean RECOVERY = false; // command line switch: indicate whether we load log from disk on startup
    protected boolean REBUILD = false;
    protected String rebuildnode = null;

    protected int PAGESIZE;

	private int ckmark = 0; // start offset of latest checkpoint. TODO: persist!!

	private FileChannel DriveChannel = null;
	private Object DriveLck = new Object();

    private long gcmark = 0; // pages up to 'gcmark' have been evicted; note, we must have gcmark <= CM.trimmark
	private int lowwater = 0, highwater = 0, freewater = -1;
	private ByteBuffer[] inmemoryStore; // use in rammode
	private byte map[] = null;
	private ByteBuffer mapb = null;
	// private static final int longsz = Long.SIZE / Byte.SIZE;
	private static final int intsz = Integer.SIZE / Byte.SIZE;
	private static final int entsz = 2*intsz;

	public void initLogStore(int sz) {
        if (RAMMODE) {
            inmemoryStore = new ByteBuffer[sz];
        }
		UNITCAPACITY = freewater = sz;
		map = new byte[sz * (entsz)];
		mapb = ByteBuffer.wrap(map);
	}

	public void initLogStore(byte[] initmap, int sz) throws Exception {
		if (RAMMODE) inmemoryStore = new ByteBuffer[sz];
		UNITCAPACITY = freewater = sz;
		map = initmap;
		mapb = ByteBuffer.wrap(map);
	}

    public SimpleLogUnitServer() {
        //default constructor
    }

    public Runnable getInstance (final Map<String,Object> config)
    {
        final SimpleLogUnitServer lut = this;

        //These are required and will throw an exception if not defined.
        lut.RAMMODE = (Boolean) config.get("ramdisk");
        lut.UNITCAPACITY = (Integer) config.get("capacity");
        lut.PORT = (Integer) config.get("port");
        lut.PAGESIZE = (Integer) config.get("pagesize");
        lut.gcmark = (Integer) config.get("trim");

        masterIncarnation = new ArrayList<Integer>();
        masterIncarnation.add(0);
        //These are not required and will be only populated if given
        if (config.containsKey("drive"))
        {
            lut.DRIVENAME = (String) config.get("drive");
            //lut.DRIVENAME= "/Users/vidhyasankaran/Documents/Project";
        }
        if (config.containsKey("recovery"))
        {
            lut.RECOVERY = (Boolean) config.get("recovery");
        }

        return new Runnable() {
            @Override
            public void run() {
                try {
                    lut.serverloop();
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        };
    }

	class mapInfo {

		int physOffset;
		int length;
		ExtntMarkType et;

		public mapInfo(long logOffset) {

			int mi = mapind(logOffset);
			mapb.position(mi);
			physOffset = mapb.getInt();

			length = mapb.getInt();
			et = ExtntMarkType.findByValue(length & 0x3); length >>= 2;
		}
	}

	public byte[] toArray() { return map; }
	public ByteBuffer toArray(long fr, int length) {
		return ByteBuffer.wrap(map, mapind(fr), length*entsz);
	}

	private int mapind(long logOffset) { // the cyclic-index converter

		int cind = (int)((logOffset+UNITCAPACITY) % UNITCAPACITY);
		return cind * entsz;
	}

	private void put(int ind, ByteBuffer buf) throws IOException {

        if (RAMMODE) {
            inmemoryStore[ind] = buf;
        } else {
            DriveChannel.position(ind*PAGESIZE);
            DriveChannel.write(buf);
        }
	}

	private boolean put(List<ByteBuffer> wbufs) throws IOException {

		if (wbufs.size() > freewater) {
			return false;
		}

		for (int j = 0; j < wbufs.size(); j++) {
			put(highwater++, wbufs.get(j));
			if (highwater >= UNITCAPACITY) highwater = 0;
		}
		freewater -= wbufs.size();
		return true;
	}

	private ArrayList<ByteBuffer> get(int pos, int sz) throws IOException {
		ArrayList<ByteBuffer> wbufs = new ArrayList<ByteBuffer>();

        if (RAMMODE) {
            for (int j = 0; j < sz; j++) {
                wbufs.add(inmemoryStore[pos++]);
                if (pos >= UNITCAPACITY) pos = 0;
            }
        } else {
            if (pos + sz > UNITCAPACITY) {
                ByteBuffer buf1 = ByteBuffer.allocate((UNITCAPACITY - pos) * PAGESIZE),
                        buf2 = ByteBuffer.allocate(((pos + sz) % UNITCAPACITY) * PAGESIZE);
                DriveChannel.read(buf1, pos * PAGESIZE);
                for (int i = 0; i < (UNITCAPACITY - pos); i++)
                    wbufs.add(ByteBuffer.wrap(buf1.array(), i * PAGESIZE, PAGESIZE));
                DriveChannel.read(buf2, 0);
                for (int i = 0; i < (pos + sz) % UNITCAPACITY; i++)
                    wbufs.add(ByteBuffer.wrap(buf2.array(), i * PAGESIZE, PAGESIZE));

            } else {
                ByteBuffer buf = ByteBuffer.allocate(sz * PAGESIZE);
                DriveChannel.read(buf, pos * PAGESIZE);
                for (int i = 0; i < sz; i++)
                    wbufs.add(ByteBuffer.wrap(buf.array(), i * PAGESIZE, PAGESIZE));
            }
        }
		return wbufs;
	}

    public ArrayList<ByteBuffer> mirror() throws IOException {
        if (highwater <= lowwater)
            return get(lowwater, highwater+UNITCAPACITY-lowwater);
        else
            return get(lowwater, highwater-lowwater);
    }

	public int getPhysOffset(long logOffset) {
		int mi = mapind(logOffset);
		return mapb.getInt(mi);
	}

	public int getLength(long logOffset) {
		int mi = mapind(logOffset) + intsz;
		return mapb.getInt(mi);
	}

	public ExtntMarkType getET(long logOffset) {
		int mi = mapind(logOffset) + intsz;
		mapb.position(mi);
		int length = mapb.getInt();
		return ExtntMarkType.findByValue(length & 0x3);
	}

	public void setExtntInfo(long logOffset, int physOffset, int length, ExtntMarkType et) throws IOException {
		int mi = mapind(logOffset);
		mapb.position(mi);
		mapb.putInt(physOffset);
		length <<= 2;
        length |= et.getValue();
		mapb.putInt(length);
        if (!RAMMODE) {
            DriveChannel.position(UNITCAPACITY * PAGESIZE + mi);
            DriveChannel.write(toArray(logOffset, 1));
        }
	}

	public void trimLogStore(long toOffset) throws IOException {
		long lasttrim = gcmark, lastcontig = lasttrim;

		log.info("=== trim({}) gcmark={} freewater={} lowwater/highwater={}/{} ===",
				toOffset, gcmark, freewater, lowwater, highwater);

		// this loop keeps advancing two markers, lasttrim and lastcontig.
        //
		// we advance both markers as long as we can free up contiguous space and move  'lowwater'.
		// lastcontig marks the point that we stop freeing up contiguous space, until we can advance lowwater again.
		// when we can move lowwater again, we go back to sweeping entries from the lastcontig mark.
        //
        // eventually, gcmark is set to the highest contiguous mark which we have freed.
        // entries above gcmark which were freed are marked internally with EX_TRIMMMED, but gcmark remains below them
        //
		while (lasttrim < toOffset) {
            mapInfo minf = new mapInfo(lasttrim);
            if (minf.et == ExtntMarkType.EX_FILLED && minf.physOffset == lowwater) {
                log.info("trim {} sz={}", lasttrim, minf.length);
                // TODO in RAMMODE, do we need to free up blocks?
                lowwater += minf.length;
                freewater += minf.length;
                if (lowwater >= UNITCAPACITY) lowwater %= UNITCAPACITY;

                // go back now to lastcontig mark
                while (lastcontig < lasttrim) {
                    minf = new mapInfo(lasttrim);
                    if (minf.et == ExtntMarkType.EX_FILLED && minf.physOffset == lowwater) {
                        log.info("trim {} sz={}", lasttrim, minf.length);
                        // TODO in RAMMODE, do we need to free up blocks?
                        lowwater += minf.length;
                        freewater += minf.length;
                        if (lowwater >= UNITCAPACITY) lowwater %= UNITCAPACITY;
                        lastcontig++;
                    } else if (minf.et != ExtntMarkType.EX_FILLED) {
                        lastcontig++;
                    } else
                        break;
                }

                // now release the extent at 'lasttrim'
                if (lastcontig == lasttrim) {
                    setExtntInfo(lasttrim, 0, 0, ExtntMarkType.EX_EMPTY);
                    lastcontig++;
                } else {
                    setExtntInfo(lasttrim, 0, 0, ExtntMarkType.EX_TRIMMED);
                }
            } else if (minf.et != ExtntMarkType.EX_FILLED) {
                if (lastcontig == lasttrim) lastcontig++;
            }

            lasttrim++;
        }

		gcmark = lastcontig;
        writegcmark();

		log.info("=== done trim({}) new gcmark={} freewater={} lowwater/highwater={}/{} ===",
				toOffset, gcmark, freewater, lowwater, highwater);

	}

	public ErrorCode appendExtntLogStore(long logOffset, List<ByteBuffer> wbufs, ExtntMarkType et)
        throws IOException {
        //TODO: figure out trim story..
		//if (logOffset < CM.getTrimmark())             return ErrorCode.ERR_OVERWRITE;
        //if ((logOffset-CM.getTrimmark()) >= UNITCAPACITY) {
        //    setExtntInfo(logOffset, 0, 0, et);
        //    return ErrorCode.ERR_FULL;
       // }

        ExtntMarkType oldet = getET(logOffset);
        if (oldet != ExtntMarkType.EX_EMPTY) {
            log.info("append would overwrite {} marked-{}", logOffset, oldet);
            return ErrorCode.ERR_OVERWRITE;
        }
		int physOffset = highwater;
		if (!put(wbufs)) {
			log.info("no free space for append({})", logOffset);
            setExtntInfo(logOffset, 0, 0, et);
			return ErrorCode.ERR_FULL;
		}
		setExtntInfo(logOffset, physOffset, wbufs.size(), et);
		return ErrorCode.OK;
	}

	public ExtntWrap getExtntLogStore(long logOffset) throws IOException {
		ExtntWrap wr = new ExtntWrap();
//TODO : figure out trim story
/*
		if (logOffset < CM.getTrimmark()) {
			wr.setErr(ErrorCode.ERR_TRIMMED);
			wr.setCtnt(new ArrayList<ByteBuffer>());
		} else if ((logOffset-CM.getTrimmark()) >= UNITCAPACITY) {
			wr.setErr(ErrorCode.ERR_UNWRITTEN);
			wr.setCtnt(new ArrayList<ByteBuffer>());
		} else {
        */
			mapInfo minf = new mapInfo(logOffset);
			wr.setInf(new ExtntInfo(logOffset, minf.length, minf.et));
			log.debug("read phys {}->{}, {}", minf.physOffset, minf.length, minf.et);
			if (minf.et == ExtntMarkType.EX_FILLED) {
				wr.setErr(ErrorCode.OK);
				wr.setCtnt(get(minf.physOffset, minf.length));
				log.debug("ctnt {}", wr.getCtnt());
			} else if (minf.et == ExtntMarkType.EX_SKIP) {
				wr.setErr(ErrorCode.OK_SKIP);
			} else if (minf.et == ExtntMarkType.EX_EMPTY) {
				wr.setErr(ErrorCode.ERR_UNWRITTEN);
			} else if (minf.et == ExtntMarkType.EX_TRIMMED) {
				wr.setErr(ErrorCode.ERR_TRIMMED);
			}
		//}
		return wr;
	}

	public ErrorCode getExtntInfoLogStore(long logOffset, ExtntInfo inf) {
        // TODO: figure out trim story
        /*
		if (logOffset < CM.getTrimmark()) {
			inf.setFlag(ExtntMarkType.EX_TRIMMED);
            return ErrorCode.ERR_TRIMMED;
		} else if ((logOffset-CM.getTrimmark()) >= UNITCAPACITY) {
			inf.setFlag(ExtntMarkType.EX_EMPTY);
            return ErrorCode.ERR_UNWRITTEN;
		} else {*/
			mapInfo minf = new mapInfo(logOffset);
            inf.setFlag(minf.et);
            inf.setMetaFirstOff(logOffset);
            inf.setMetaLength(minf.length);
            switch (minf.et) {
                case EX_FILLED: return ErrorCode.OK;
                case EX_TRIMMED: return ErrorCode.ERR_TRIMMED;
                case EX_EMPTY: return ErrorCode.ERR_UNWRITTEN;
                case EX_SKIP: return ErrorCode.ERR_UNWRITTEN;
                default: log.error("internal error in getExtntInfoLogStore"); return ErrorCode.ERR_BADPARAM;
            }
		//}
	}

    private void writegcmark() throws IOException {
        // TODO what about persisting the configuration??
        if (!RAMMODE) {
            DriveChannel.position(UNITCAPACITY * PAGESIZE + UNITCAPACITY * entsz);
            byte[] ser = Util.ObjectSerialize(new Long(gcmark));
            DriveChannel.write(ByteBuffer.wrap(ser));
        }
    }

    private void recover() throws Exception {

        long filesz =  DriveChannel.size();
        int sz = Util.ObjectSerialize(new Long(0)).length; // size of extra info after bitmap

        DriveChannel.position(UNITCAPACITY*PAGESIZE);
        ByteBuffer bb = ByteBuffer.allocate(UNITCAPACITY*entsz);
        DriveChannel.read(bb);
        ByteBuffer tb = ByteBuffer.allocate(sz);
        DriveChannel.position(UNITCAPACITY*PAGESIZE+ UNITCAPACITY*entsz);
        if (DriveChannel.read(tb) == sz) {
            gcmark = ((Long) Util.ObjectDeserialize(tb.array())).longValue();
            log.debug("trimmark recovered: {}", gcmark);
        } else {
            log.info("no gcmark saved, setting initial trim=0");
            gcmark=0;
            writegcmark();
        }
        initLogStore(bb.array(), UNITCAPACITY);
    }
/*
    private void rebuildfromnode() throws Exception {
        Endpoint cn = Endpoint.genEndpoint(rebuildnode);
        TTransport buildsock = new TSocket(cn.getHostname(), cn.getPort());
        buildsock.open();
        TProtocol prot = new TBinaryProtocol(buildsock);
        TMultiplexedProtocol mprot = new TMultiplexedProtocol(prot, "CONFIG");

        SimpleLogUnitConfigService.Client cl = new SimpleLogUnitConfigService.Client(mprot);
        log.info("established connection with rebuild-node {}", rebuildnode);
        SimpleLogUnitWrap wr = null;
        try {
            wr = cl.rebuild();
            log.info("obtained mirror lowwater={} highwater={} trimmark={} ctnt-length={}",
                    wr.getLowwater(), wr.getHighwater(), wr.getTrimmark(), wr.getCtntSize());
            initLogStore(wr.getBmap(), UNITCAPACITY);
            lowwater = highwater = wr.getLowwater();
            gcmark = wr.getTrimmark();
            ckmark = (int)wr.getCkmark();
            put(wr.getCtnt());
            if (highwater != wr.getHighwater())
                log.error("rebuildfromnode lowwater={} highwater={} received ({},{})",
                        lowwater, highwater,
                        wr.getLowwater(), wr.getHighwater());
        } catch (TException e) {
            e.printStackTrace();
        }
    }*/
    
    @Override
    public boolean ping() throws TException {
        return true;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////
	/* (non-Javadoc)
	 * implements to CorfuUnitServer.Iface write() method.
	 * @see CorfuUnitServer.Iface#write(ExtntWrap)
	 *
	 * we make great effort for the write to either succeed in full, or not leave any partial garbage behind.
	 * this means that we first check if all the pages to be written are free, and that the incoming entry contains content for each page.
	 * in the event of some error in the middle, we reset any values we already set.
	 */
	@Override
	synchronized public ErrorCode write(UnitServerHdr hdr, List<ByteBuffer> ctnt, ExtntMarkType et) throws org.apache.thrift.TException {

        if (Util.compareIncarnations(hdr.getEpoch(), masterIncarnation) < 0) {
            log.info("write request has stale incarnation={} cur incarnation={}",
                    hdr.getEpoch(), masterIncarnation);
            return ErrorCode.ERR_STALEEPOCH;
        }

		log.debug("write({} size={} marktype={})", hdr.off, ctnt.size(), et);
        try {
            return appendExtntLogStore(hdr.off, ctnt, et);
        } catch (IOException e) {
            e.printStackTrace();
            return ErrorCode.ERR_IO;
        }
    }

	/**
	 * mark an extent 'skipped'
	 * @param hdr epoch and offset of the extent
	 * @return OK if succeeds in marking the extent for 'skip'
	 * 		ERROR_TRIMMED if the extent-range has already been trimmed
	 * 		ERROR_OVERWRITE if the extent is occupied (could be a good thing)
	 * 		ERROR_FULL if the extent spills over the capacity of the log
	 * @throws TException
	 */
	@Override
	synchronized public ErrorCode fix(UnitServerHdr hdr) throws TException {
		return write(hdr, new ArrayList<ByteBuffer>(), ExtntMarkType.EX_SKIP);
	}

    private ExtntWrap genWrap(ErrorCode err) {
        return  new ExtntWrap(err, new ExtntInfo(), new ArrayList<ByteBuffer>());
    }

	/* (non-Javadoc)
	 * @see CorfuUnitServer.Iface#read(org.corfudb.CorfuHeader, ExtntInfo)
	 *
	 * this method performs actual reading of a range of pages.
	 * it fails if any page within range has not been written.
	 * it returns OK_SKIP if it finds any page within range which has been junk-filled (i.e., the entire range becomes junked).
	 *
	 * the method also reads-ahead the subsequent meta-info entry if hdr.readnext is set.
	 * if the next meta info record is not available, it returns the current meta-info structure
	 *
	 *  @param a CorfuHeader describing the range to read
	 */
	@Override
	synchronized public ExtntWrap read(UnitServerHdr hdr) throws org.apache.thrift.TException {
        if (Util.compareIncarnations(hdr.getEpoch(), masterIncarnation) < 0) return genWrap(ErrorCode.ERR_STALEEPOCH);
		log.debug("read({})", hdr.off);
        try {
            return getExtntLogStore(hdr.off);
        } catch (IOException e) {
            e.printStackTrace();
            return genWrap(ErrorCode.ERR_IO);
        }
    }

	/* read the meta-info record at specified offset
	 *
	 * @param off- the offset to read from
	 * @return the meta-info record "wrapped" in ExtntWrap.
	 *         The wrapping contains error code: UNWRITTEN if reading beyond the tail of the log
	 *
	 * (non-Javadoc)
	 * @see CorfuUnitServer.Iface#readmeta(long)
	 */
	@Override
	synchronized public ExtntWrap readmeta(UnitServerHdr hdr) {
        if (Util.compareIncarnations(hdr.getEpoch(), masterIncarnation) < 0) return genWrap(ErrorCode.ERR_STALEEPOCH);
		log.debug("readmeta({})", hdr.off);
		ExtntInfo inf = new ExtntInfo();
		return new ExtntWrap(getExtntInfoLogStore(hdr.off, inf), inf, new ArrayList<ByteBuffer>());
	}

	/**
	 * wait until any previously written log entries have been forced to persistent store
	 */
    @Override
	synchronized public void sync() throws org.apache.thrift.TException {
    	synchronized(DriveLck) { try { DriveLck.wait(); } catch (Exception e) {
    		log.error("forcing sync to persistent store failed, quitting");
    		System.exit(1);
    	}}
    }

	@Override
	synchronized public long querytrim() {
        //return CM.getTrimmark();
        //TODO figure out trim story
        return 0;
        }

	@Override
	synchronized public long queryck() {	return ckmark; }

	ErrorCode trim(long toOffset) {
        try {
            trimLogStore(toOffset);
        } catch (IOException e) {
            e.printStackTrace();
            return ErrorCode.ERR_IO;
        }
        if (!RAMMODE) {
	    	try {
	    	   	log.debug("forcing bitmap and gcmark to disk");
	    	   	synchronized(DriveLck) {
	    	   		try { DriveLck.wait(); } catch (InterruptedException e) {
		        		log.error("forcing sync to persistent store failed, quitting");
		        		System.exit(1);
	    	   		}
	        	}
	    	    writegcmark();
			} catch (IOException e) {
				log.error("writing gcmark failed");
				e.printStackTrace();
				return ErrorCode.ERR_IO;
			}
    	}
    	return ErrorCode.OK;
	}

	@Override
    synchronized public void ckpoint(UnitServerHdr hdr) throws org.apache.thrift.TException {
        // if (hdr.getEpoch() < epoch) return ErrorCode.ERR_STALEEPOCH;
		log.info("mark latest checkpoint offset={}", hdr.off);
		if (hdr.off > ckmark) ckmark = (int) (hdr.off % UNITCAPACITY);
	}

    /**
     * class implementing the configServer service
     */
    class LogUnitConfigServiceImpl implements SimpleLogUnitConfigService.Iface {

        @Override
        public void probe() throws TException {
            ;
        }

        @Override
        synchronized public ErrorCode phase2b(String xmlconfig) {
            /*
            try {
                CorfuConfiguration nc = new CorfuConfiguration(xmlconfig);
                int cmp = Util.compareIncarnations(masterIncarnation, nc.getIncarnation());
                if (cmp > 0)
                    return ErrorCode.ERR_STALEEPOCH;
                if (cmp == 0)
                    return ErrorCode.OK;

                log.info("set new configuration: {}", xmlconfig);
            //    if (nc.getTrimmark() > CM.getTrimmark())
            //        trim(nc.getTrimmark());
          //      CM = nc;
                // TODO persist?
                return ErrorCode.OK;
            } catch (CorfuException e) {
                e.printStackTrace();
                return ErrorCode.ERR_IO;
            }
            */
            return ErrorCode.OK;
        }

        @Override
        synchronized public String phase1b(int masterid) {
            String s = null;
            /*
            if (CM != null) {
                try {
                    s = CM.ConfToXMLString();
                } catch (CorfuException e) {
                    e.printStackTrace();
                }
            }
            if (!Util.getMasterId(masterIncarnation).equals(masterid))
                Util.incMasterEpoch(masterIncarnation, masterid);
                */
            return s;
        }

        @Override
        synchronized public SimpleLogUnitWrap rebuild() throws TException {

            SimpleLogUnitWrap wr = new SimpleLogUnitWrap(ErrorCode.OK,
                    lowwater, highwater,
                    gcmark, ckmark,
                    null,
                    mapb);
            log.info("respond to rebuild request. lowwater={}, highwater={}, trimmark={}",
                    lowwater, highwater, gcmark);

            try {
                wr.setCtnt(mirror());
            } catch (IOException e) {
                log.error("rebuild request failed");
                e.printStackTrace();
                wr.setErr(ErrorCode.ERR_IO);
            }
            return wr;
        }

        @Override
        synchronized public void kill() {
            log.warn("@C@ bye bye!");
            // throw new ThreadDeath();
            System.exit(1);
        }
    }

    //////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////


    public void serverloop() throws Exception {

        log.warn("@C@ CorfuLoggingUnit starting");

        if (!RAMMODE) {
            try {
                RandomAccessFile f = new RandomAccessFile(DRIVENAME, "rw");
                if (!RECOVERY) f.setLength(0);
                DriveChannel = f.getChannel();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(1); // not much to do without storage...
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1); // not much to do without storage...
            }

            // fork off a thread to constantly force syncing to disk
            //
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (; ; ) {
                        try {
                            DriveChannel.force(false);
                            synchronized (DriveLck) {
                                DriveLck.notifyAll();
                            }
                            Thread.sleep(1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        } else {
            inmemoryStore = new ByteBuffer[UNITCAPACITY];
        }

        if (RECOVERY) {
            recover();
        } else if (REBUILD) {
            log.warn("REBUILD TEMPORARILY DISABLED");
            //TODO reimplement rebuild
            //rebuildfromnode();
        } else {
            initLogStore(UNITCAPACITY);
            writegcmark();
        }

        TServer server;
        TServerSocket serverTransport;
        System.out.println("run..");

        try {
            serverTransport = new TServerSocket(PORT);

            LogUnitConfigServiceImpl cnfg = new LogUnitConfigServiceImpl();

            TMultiplexedProcessor mprocessor = new TMultiplexedProcessor();
            mprocessor.registerProcessor("SUNIT", new SimpleLogUnitService.Processor<SimpleLogUnitServer>(this));
            mprocessor.registerProcessor("CONFIG", new SimpleLogUnitConfigService.Processor<LogUnitConfigServiceImpl>(cnfg));

            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(mprocessor));
            System.out.println("Starting Corfu storage unit server on multiplexed port " + PORT);

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
