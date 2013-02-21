package com.ztesoft.cep.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.StringWriter;

import org.apache.log4j.Logger;
import com.ztesoft.cep.model.LoadTask;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FileUtils {
	static Logger logger = Logger.getLogger(FileUtils.class.getName());
	static private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S");

	public static void deleteCepDataFile(String filename) {
		logger.debug("try delete date file " + filename);
		File datafile = new File(filename);
		File lockfile = new File(datafile.getAbsolutePath() + ".lock");
		FileLockUtil.ReturnPair ret = FileLockUtil.counting(lockfile.getAbsolutePath(), -1);
		logger.debug(".lock  FileLockUtil.counting(" + datafile.getAbsoluteFile()
				+ ") current num is [" + ret.curr_count + "]");
		if ((ret.oper_success == true && ret.curr_count == 0) || ret.oper_success == false) {
			if (datafile.delete() == false) {
				logger.error("datafile file[" + datafile.getAbsolutePath()
						+ "] delete failure after load successful");
			}
			if (lockfile.delete() == false) {
				logger.error("lock file[" + lockfile.getAbsolutePath()
						+ "] delete failure after load successful");
			}
		}
	}

	public static boolean isExtension(String filename, String ext) {
		if (ext == null || ext.equals("")) {
			logger.error("ext is null");
			return false;
		}
		if ((filename != null) && (filename.length() > 0)) {
			int i = filename.lastIndexOf('.');
			if ((i > -1) && (i < (filename.length() - 1))) {
				return filename.substring(i + 1).equals(ext);
			}
		}
		return false;
	}

	public static String getExtension(String filename, String defExt) {
		if ((filename != null) && (filename.length() > 0)) {
			int i = filename.lastIndexOf('.');

			if ((i > -1) && (i < (filename.length() - 1))) {
				return filename.substring(i + 1);
			}
		}
		return defExt;
	}

	public static boolean mkdirsWithAccess(String path) {
		File doingdir = new File(path);
		if (doingdir.exists() == false) {
			if (doingdir.mkdirs() == false) {
				logger.error("create dir " + doingdir.getAbsolutePath() + "failure!");
				return false;
			}
		}
		if (!doingdir.isDirectory() || !doingdir.canWrite() || !doingdir.canExecute()
				|| !doingdir.canRead()) {
			logger.error("dir " + doingdir.getAbsolutePath() + " access denied");
			return false;
		}
		return true;
	}

	public static void openDeleteClose(String filename) {
		File files = new File(filename);
		if (!files.exists()) {
			logger.error("file[" + filename + "] not exist!");
			return;
		}
		RandomAccessFile file = null;
		Process proc = null;
		try {
			file = new RandomAccessFile(filename, "rw");
			proc = Runtime.getRuntime().exec("unlink " + filename);
			proc.waitFor();
		} catch (IOException e) {
			logger.error("file[" + filename + "] ", e);
		} catch (InterruptedException e) {
			logger.error("file[" + filename + "] ", e);
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
					logger.error("file[" + filename + "] close error");
				}
			}
			if (proc != null) {
				proc.destroy();
			}
		}
	}

	public static LoadTask getCommandFromString(String commandstr, String delimiter,
			int cmd_field_count) {
		String s[] = commandstr.split(delimiter);
		LoadTask task = new LoadTask();
		if (s != null) {
			if (s.length == cmd_field_count) {
				// command.setPeid(Integer.parseInt(s[0]));
				task.setEventtype(Integer.parseInt(s[1]));
				task.setDestFilePath(s[2]);
				try {
					task.setContentTimestamp(df.parse(s[3]));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					logger.error("getCommandFromString  parse timewindow error", e);
				}
				// command.setTimestamp(s[4]);
				// command.setCycle(s[5]);
			} else {
				logger.error("unsupport command format[" + commandstr + "]");
				return null;
			}
		} else {
			logger.error("unsupport command format[" + commandstr + "]");
			return null;
		}
		logger.debug(task.toString());
		return task;
	}

	public static long getFilesize(String filename) {
		long filesize = 0;
		File f = new File(filename);
		if (f.isFile()) {
			filesize = f.length();
		}
		return filesize;
	}

	public static List<LoadTask> getCommandFromFile(File commandfile, String delimiter,
			int cmd_field_count) {
		List<LoadTask> ret_list = null;
		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(commandfile));
			try {
				String command = in.readLine();
				logger.info("cmd[" + commandfile.getAbsolutePath() + "] content[" + command + "]");
				if (command == null) {
					logger.debug("command is null");
					return ret_list;
				}
				while (command != null) {
					command = command.replaceAll("\"", "");
					LoadTask com = getCommandFromString(command, delimiter, cmd_field_count);
					if (com != null) {
						ret_list = new ArrayList<LoadTask>();
						ret_list.add(com);
					}
					command = in.readLine();
				}
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage(), e);
		} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
		}
		return ret_list;
	}

	public static String file2String(File file, String encoding) {
		InputStreamReader reader = null;
		StringWriter writer = new StringWriter();
		try {
			if (encoding == null || "".equals(encoding.trim())) {
				reader = new InputStreamReader(new FileInputStream(file), encoding);
			} else {
				reader = new InputStreamReader(new FileInputStream(file));
			}
			// write the inputstream to outputstream
			char[] buffer = new char[65535];
			int n = 0;
			while (-1 != (n = reader.read(buffer))) {
				writer.write(buffer, 0, n);
			}
		} catch (Exception e) {
			logger.error("file2String error", e);
			return null;
		} finally {
			if (reader != null)
				try {
					reader.close();
				} catch (IOException e) {
					logger.error("file2String close error", e);
				}
		}
		return writer.toString();
	}

	public static boolean taskCmdMove(LoadTask task, String destPath) {
		String filename = task.getCommadnFile().getName();
		String destfilename = destPath + File.separator + filename;
		boolean ret = FileUtils.rename(task.getCommadnFile().getAbsolutePath(), destfilename);
		if (ret) {
			task.setCommadnFile(new File(destfilename));
		}
		return ret;
	}

	public static boolean rename(String srcfilename, String dstfilename) {
		File src = new File(srcfilename);
		File dst = new File(dstfilename);
		boolean retval = false;
		if (!src.getAbsolutePath().equals(dst.getAbsolutePath())) {
			// FileUtils.copyFile(src, dst);
			Process proc = null;
			try {
				proc = Runtime.getRuntime().exec(
						"mv " + src.getAbsolutePath() + " " + dst.getAbsoluteFile());
				logger.info("mv " + src.getAbsolutePath() + " " + dst.getAbsoluteFile());
				try {
					proc.waitFor();
				} catch (InterruptedException e) {
					proc.destroy();
					logger.error("rename " + srcfilename + " to " + dstfilename
							+ " fail dou to exception", e);
				}
				if (proc.exitValue() == 0) {
					retval = true;
				} else {
					InputStream errstr = proc.getErrorStream();
					byte[] err = new byte[1024];
					errstr.read(err);
					logger.error(new String(err));
					errstr.close();
				}
			} catch (IOException e) {
				logger.error("rename " + srcfilename + " to " + dstfilename
						+ " fail dou to exception", e);
			} finally {
				if (proc != null)
					proc.destroy();
			}
		} else {
			retval = true;
		}
		return retval;
	}

}
