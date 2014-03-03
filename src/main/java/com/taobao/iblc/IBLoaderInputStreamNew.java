package com.taobao.iblc;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.conf.IBLConfig;
import com.taobao.iblc.pojo.DataBlock;
import com.taobao.iblc.pojo.DataPack;

public class IBLoaderInputStreamNew extends InputStream {
	protected final static Logger logger = LoggerFactory
			.getLogger(IBLoaderInputStream.class);
	
	private BlockingQueue<DataPack> queue = null;
	
	private String actualTableName = null;
	
	private int maxReadByteCount = 0;
	private int readByteCount = 0;
	
	private LinkedList<DataBlock> dataArea; 
	private byte buffer[] = null;
	private int buffer_len = 0;
	private int buffer_off = 0;
	//private static String charset = IBLConfig.charset;

	public IBLoaderInputStreamNew(BlockingQueue<DataPack> queue) {
		super();
		this.queue = queue;
	}
	
	public void init(){
		DataPack curDP = null;
		try {
			curDP = queue.take();
		} catch (InterruptedException e) {
			logger.error("InterruptedException!!!", e);
		}
		this.actualTableName = curDP.getActualTableName();
		//buildBuffer(curDP);
		this.dataArea = curDP.getDataArea();
		buildBuffer(this.dataArea);
		curDP = null;
		this.readByteCount = 0;
		this.maxReadByteCount = IBLConfig.maxReadByteCount;
	}

	@Override
	public int read() throws IOException {
		throw new IOException("Read() is not supported");
	}
	
	@Override
	public int read(byte b[], int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        
        int total = 0;
        int read = 0;
        while (len > 0) {
            read = this.readOneTime(b, off, len);
            if (read < 0) {
                break;
            }
            off += read;
            len -= read;
            total += read;
        }
        
        if (total == 0)
          return (-1);

        return total;

	}

	private int readOneTime(byte[] b, int off, int len) {

		if (this.buffer_len == 0) {
			if (buildBuffer(this.dataArea) != 0) {
				if (this.readByteCount < this.maxReadByteCount) {
					DataPack dp = queue.peek();
					if (dp != null && !dp.getActualTableName().equals(
									this.getActualTableName()))
						return (-1);
					DataPack curDP = queue.poll();
					if (curDP != null) {
						this.dataArea = curDP.getDataArea();
						buildBuffer(this.dataArea);
						curDP = null;
					} else {
						return (-1);
					}
				} else {
					return (-1);
				}
			}
		}

		int readlen = Math.min(this.buffer_len, len);
		System.arraycopy(buffer, buffer_off, b, off, readlen);
		len = len - readlen;
		off = off + readlen;
		this.buffer_len = this.buffer_len - readlen;
		this.buffer_off = this.buffer_off + readlen;

		this.readByteCount = this.readByteCount + readlen;

		return readlen;

	}

	private void buildBuffer(DataPack curDP){
	    this.buffer = curDP.getBuf();		
		this.buffer_len = this.buffer.length;
		this.buffer_off = 0;
	}
	
	private int buildBuffer(LinkedList<DataBlock> dataArea){
		if(dataArea.peek() == null){
			return -1;
		}
		DataBlock db = dataArea.removeFirst();
	    this.buffer = db.getData();		
		this.buffer_len = db.getLen();
		this.buffer_off = 0;
		//System.out.println("buildBuffer :"+this.buffer_len);
		return 0;
	}
	
	public String getActualTableName() {
		return actualTableName;
	}

	public void setActualTableName(String actualTableName) {
		this.actualTableName = actualTableName;
	}
	
}
