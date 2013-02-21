package com.ztesoft.cep.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.ztesoft.cep.DataLoader;

// import java.util.Arrays;

/**
 * Created on 2012-6-16 PM 03:09:22 WangquN
 */

/**
 * <br>
 * errorCode in [-00000000, -99999999]<br>
 * 
 * @author WangquN
 * @version 0.1<br>
 *          2012-6-16 PM 03:09:22
 */
public class FileLockUtil {
	public static final int len4buf = 16;
	static Logger logger = Logger.getLogger(DataLoader.class.getName());

	static public class ReturnPair {
		public boolean oper_success;
		public int curr_count;
	}

	/**
	 * <br>
	 * <h>Business logic args detail list:</h> <li>IN:<br>
	 * </li> <li>OUT:<br>
	 * </li>
	 * 
	 * @param args
	 */

	public static ReturnPair counting(String filename, Integer count) {
		// logger.debug("begin counting(" + filename + ", " + count + ")!");
		RandomAccessFile raf = null;
		FileChannel fc = null;
		FileLock fl = null;
		FileLockUtil.ReturnPair retpair = new FileLockUtil.ReturnPair();
		retpair.oper_success = false;
		try {
			raf = new RandomAccessFile(filename, "r");
			logger.debug("counting end of new RandomAccessFile!");
			fc = raf.getChannel();
			logger.debug("counting end of getChannel!");
			// ByteBuffer bb = ByteBuffer.allocate(8);
			byte[] buff = new byte[len4buf];
			// Arrays.fill(buff, '0');
			// for (int i = 0; i < 16; ++i)
			// buff[i] = '0';
			if (fc.size() == 0) {
				logger.debug("lock file[" + filename + "] size is 0!");
				retpair.curr_count = 0;
				retpair.oper_success = false;
				return retpair;
			} else {
				// fc.read(bb);
				// bb.flip();
				// long currValue = bb.getLong();
				// bb.clear();
				// bb.putLong(currValue + _cache);
				// bb.rewind();
				// fc.position(0);
				// fc.write(bb);

				// / int currValue = raf.readInt();
				// / raf.seek(0);
				// / raf.writeInt(currValue + 1);
				raf.read(buff);
				logger.debug("counting end of read buff!");
				int count_ = 0;
				// count for content length
				for (; ((buff[count_] >= '0' && buff[count_] <= '9') || buff[count_] == '-' || buff[count_] == '+')
						&& count_ < len4buf; ++count_)
					;
				if (count_ == 1) {
					retpair.curr_count = 0;
					retpair.oper_success = true;
					return retpair;
				}
			}
		} catch (FileNotFoundException e) {
			// ignore exception for constructor RandomAccessFile!
			logger.debug(filename, e);
		} catch (IOException e) {
			// ignore exception for fc.lock!
			logger.error(filename, e);
		} finally {
			if (fl != null)
				try {
					fl.release();
				} catch (IOException e1) {
					// ignore exception!
				}
			if (fc != null && fc.isOpen())
				try {
					fc.close();
				} catch (IOException e) {
					// ignore exception!
				}
			if (raf != null)
				try {
					raf.close();
				} catch (IOException e) {
					// ignore exception!
				}
		}
		try {
			logger.debug("counting new RandomAccessFile(" + filename + ", " + count + ")!");
			raf = new RandomAccessFile(filename, "rws");
			logger.debug("counting end of new RandomAccessFile!");
			fc = raf.getChannel();
			logger.debug("counting end of getChannel!");
			fl = fc.lock(0L, len4buf, false);
			logger.debug("counting end of lock!");
			// ByteBuffer bb = ByteBuffer.allocate(8);
			byte[] buff = new byte[len4buf];
			// Arrays.fill(buff, '0');
			// for (int i = 0; i < 16; ++i)
			// buff[i] = '0';
			if (fc.size() == 0) {
				logger.debug("lock file[" + filename + "] size is 0!");
			} else {
				// fc.read(bb);
				// bb.flip();
				// long currValue = bb.getLong();
				// bb.clear();
				// bb.putLong(currValue + _cache);
				// bb.rewind();
				// fc.position(0);
				// fc.write(bb);

				// / int currValue = raf.readInt();
				// / raf.seek(0);
				// / raf.writeInt(currValue + 1);
				raf.read(buff);
				logger.debug("counting end of read buff!");
				int count_ = 0;
				// count for content length
				for (; ((buff[count_] >= '0' && buff[count_] <= '9') || buff[count_] == '-' || buff[count_] == '+')
						&& count_ < len4buf; ++count_)
					;
				if (count_ == 0) {
					logger.error("lock file[" + filename + "] char nums is 0!");
					// buff[0] = '1';
					// System.out.println("directly count = 1");
				} else {
					String strBuff = new String(buff, 0, count_);
					int currValue = Integer.parseInt(strBuff);
					currValue += count;
					/*
					 * System.out.println("read buff:" + strBuff + ". size is:"
					 * + strBuff.length() + ". currValue is:" + currValue);
					 */
					byte[] buff2 = Integer.toString(currValue).getBytes();
					for (int i = 0; i < len4buf; ++i) {
						if (i < buff2.length)
							buff[i] = buff2[i];
						else
							buff[i] = 0;
					}
					// System.out.println("count = " + currValue);
					raf.seek(0);
					raf.write(buff);
					retpair.curr_count = currValue;
					retpair.oper_success = true;
				}
				// int currValue = Integer.parseInt(new String(buff));
				// System.out.println("count = " + currValue);
				// buff = String.format("%016d", (currValue +
				// 1)).getBytes();

				// fc.force(false);
				// bb.clear();
				// fl.release();
				// fc.close();
				// raf.close();
			}
		} catch (FileNotFoundException e) {
			// ignore exception for constructor RandomAccessFile!
			logger.error(filename, e);
		} catch (IOException e) {
			// ignore exception for fc.lock!
			logger.error(filename, e);
		} finally {
			if (fl != null)
				try {
					fl.release();
				} catch (IOException e1) {
					// ignore exception!
				}
			if (fc != null && fc.isOpen())
				try {
					fc.close();
				} catch (IOException e) {
					// ignore exception!
				}
			if (raf != null)
				try {
					raf.close();
				} catch (IOException e) {
					// ignore exception!
				}
		}
		logger.debug("end counting(" + filename + ", " + count + ")!");
		return retpair;
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf"
				+ File.separator + "log4j.properties");
		Integer count = new Integer(-1);
		FileLockUtil.ReturnPair retval = FileLockUtil.counting("D:\\a.lock", count);
		System.out.println("" + retval.oper_success + retval.curr_count);
	}

}
