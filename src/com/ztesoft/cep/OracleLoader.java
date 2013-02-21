package com.ztesoft.cep;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.LoadTask;
import com.ztesoft.cep.utils.FileUtils;

public class OracleLoader extends DataLoader {

	public OracleLoader() {
	}

	public static int ctlfileid = 0;

	@Override
	public void load(LoadTask task) {
		try {
			if (executeLoad(task) == true) {
				logger.info("loaded successful...");
				this.setCmdResult(task, LoadTask.TASKSTATE.SUCCESSFUL);
			}
		} catch (Exception e) {
			logger.error("execute load failure ", e);
		}
	}

	String getLoadStr(File ctlfile) {
		String load_scheme = "sqlldr userid=" + LoadMaster.instance().config.user + "/"
				+ LoadMaster.instance().config.password;
		if (LoadMaster.instance().config.tns_alias != null
				&& !LoadMaster.instance().config.tns_alias.equals("")) {
			load_scheme = load_scheme + "@" + LoadMaster.instance().config.tns_alias;
		}
		load_scheme = load_scheme + ",control=" + ctlfile.getAbsolutePath() + " log="
				+ ctlfile.getAbsolutePath() + ".log "
				+ " silent=feedback direct=true  multithreading=true";
		// System.out.println("loader shell is " + load_scheme);
		logger.info(load_scheme);
		return load_scheme;
	}

	File createCtlFile(LoadTask task) {
		Date d = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_hh_mm_ss_S");
		LoadConfig config = task.getConfig();
		String filename = "ctl_" + config.getEventtype() + "_" + config.getCycle() + "_"
				+ (ctlfileid++) + "_" + sdf.format(d) + ".ctl";
		// System.out.println(filename);
		File ctlfile = new File(LoadMaster.instance().config.getCommdoing() + File.separator
				+ filename);
		StringBuilder sb = new StringBuilder();
		sb.append("load data\n");
		sb.append("infile '" + task.getDestFilePath() + "'\n");
		sb.append("append\n");
		sb.append("continueif last !='" + "\"'\n");
		sb.append("into table " + task.getDestTablename() + "\n");
		sb.append("fields terminated by ','\n");
		sb.append("optionally enclosed by '\"'\n(");
		for (int i = 0; i < config.getTableSchema().fields.size(); i++) {
			if (i != config.getTableSchema().fields.size() - 1) {
				sb.append(config.getTableSchema().fields.get(i).name + ",\n");
			} else {
				sb.append(config.getTableSchema().fields.get(i).name + ")");
			}
		}
		try {
			FileWriter fw = new FileWriter(ctlfile);
			fw.write(sb.toString());
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("control file create failure !!!" + e.getMessage(), e);
		}
		return ctlfile;
	}

	@SuppressWarnings("unused")
	boolean executeLoad(LoadTask task) {
		File ctlfile = createCtlFile(task);
		File src_log = new File(LoadMaster.instance().config.getCommdoing() + File.separator
				+ ctlfile.getName() + ".log");
		String loadstr = getLoadStr(ctlfile);
		task.setLoadSchema(loadstr);
		Process proc = null;
		try {
			proc = Runtime.getRuntime().exec(loadstr);
			try {
				proc.waitFor();
				proc.destroy();
			} catch (InterruptedException e) {
				proc.destroy();
				this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
				task.setStateInfo(e.getMessage());
				logger.error("sqlldr command exec failure", e);
				ctlfile.delete();
				src_log.delete();
				return false;
			}
			if (proc.exitValue() == 0) {
				ctlfile.delete();
				src_log.delete();
				return true;
			}

			this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);

			logger.warn("sqlldr command exec ret val[" + proc.exitValue() + "]!=0");
			task.setStateInfo(FileUtils.file2String(src_log, ""));
			src_log.delete();
			ctlfile.delete();
			return false;
		} catch (IOException e) {
			if (proc != null)
				proc.destroy();
			task.setStateInfo(e.getMessage());
			// TODO Auto-generated catch block
			this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
			logger.error("sqlldr command exec exception", e);
			return false;
		}

	}

	/*
	 * 1196243745000
	 */
	public static void main(String argv[]) {
		// try {
		// Class.forName("oracle.jdbc.OracleDriver");
		// } catch (ClassNotFoundException e) {
		// System.err.println("Can not find the DB Driver: ");
		// return;
		// }
		// LoadMaster.instance();
		// LoadCommand lc = new LoadCommand();
		// lc.setEventtype(0x1404);
		// lc.setCycle("mon");
		// lc.setTimestamp("2012-04-17 12:13:32.000");
		// lc.setFilepath("/zsmart/etl72_dev/test/wugangtest/test.file");
		// DataLoader sl = new OracleLoader(0);
		// sl.init();
		// sl.load(lc);
		// System.out.println(lc.getLoadresultstr());
	}

}
