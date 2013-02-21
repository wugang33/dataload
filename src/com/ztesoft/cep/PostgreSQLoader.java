package com.ztesoft.cep;

import java.io.IOException;
import java.util.Date;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.LoadTask;
import org.springframework.jdbc.core.JdbcTemplate;

public class PostgreSQLoader extends DataLoader {
	public static class ExtendTableConfig {
		public static String location = "10.45.50.72:8081";
		public static String ip = "10.45.50.72";
		public static int port = 8081;
		public static String gpfdistpath = "/home/gpadmin/";
		public static String format = "CSV";
		public static String logerror = "err_expenses";
		public static int rejectcount = 10000;
		public static String rejectmode = "ROWS";// or precent;
		public static String encoding = "UTF-8";
		public static boolean header = false;
		public static String delimiter;
		public static String escape;
		public static String newline;
		public static String quote;

		public static void init() {

		}

		public static String getConfigStr(String filename) {
			String replaced = filename.replace(gpfdistpath, "");
			if (replaced == filename) {
				System.out.println("filename is not in gpfdistpath");
				return null;
			}
			String configstr = "location ('gpfdist://" + location + "/" + replaced + "')\n";
			configstr = configstr + "format '" + format + "'\n";
			configstr = configstr + "encoding '" + encoding + "'\n";
			configstr = configstr + "log errors into " + logerror + " segment reject limit "
					+ rejectcount + " " + rejectmode;
			System.out.println("configstr is " + configstr);
			return configstr;
		}

	}

	@Override
	public boolean init(int id) {
		try {
			Process p = Runtime.getRuntime().exec(
					"gpfdist -d " + ExtendTableConfig.gpfdistpath + " -p " + ExtendTableConfig.port
							+ " -l gpfdist.log &");
			try {
				p.waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return super.init(id);

	}

	private static int extend_table_uuid = 0;

	public int getExtendUUID() {
		extend_table_uuid++;
		if (extend_table_uuid < 0) {
			extend_table_uuid = 0;
		}
		return extend_table_uuid;
	}

	public PostgreSQLoader() {
	}

	@Override
	public void load(LoadTask task) {
		try {
			if (executeLoad(jdbc, task) == true) {
				this.setCmdResult(task, LoadTask.TASKSTATE.SUCCESSFUL);
			}
		} catch (Exception e) {
		}
	}

	String getLoadStr(LoadTask task, String externalTabneName) {
		String load_schema = "insert into " + task.getDestTablename() + " select * from "
				+ externalTabneName;
		return load_schema;
	}

	String createExtendTable(JdbcTemplate jdbc, LoadTask task) {
		LoadConfig config = task.getConfig();
		String extendtablename = "ext_" + config.getEventtype() + "_" + config.getCycle()
				+ (new Date().getTime()) + getExtendUUID();
		String create_schema = "create external table " + extendtablename + " (\n";
		for (int i = 0; i < config.getTableSchema().fields.size(); i++) {
			if (i != (config.getTableSchema().fields.size() - 1)) {
				create_schema = create_schema + config.getTableSchema().fields.get(i).name + " "
						+ config.getTableSchema().fields.get(i).type + ",\n";
			} else {
				create_schema = create_schema + config.getTableSchema().fields.get(i).name + " "
						+ config.getTableSchema().fields.get(i).type + ")\n";
			}
		}
		String configstr = ExtendTableConfig.getConfigStr(task.getDestFilePath());
		if (configstr == null) {
			return null;
		}
		create_schema += configstr;
		jdbc.execute(create_schema);
		System.out.println("external table schema \n" + create_schema);
		return extendtablename;
	}

	boolean dropExtendTable(JdbcTemplate jdbc, String extendtablename) {
		jdbc.execute("drop external table " + extendtablename);
		return true;
	}

	boolean executeLoad(JdbcTemplate jdbc, LoadTask task) {
		String extendtablename = null;
		try {
			extendtablename = createExtendTable(jdbc, task);
			if (extendtablename == null) {
				this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
				logger.error("create external table failure");
				return false;
			}
			String loadstr = getLoadStr(task, extendtablename);
			jdbc.execute(loadstr);
			this.setCmdResult(task, LoadTask.TASKSTATE.SUCCESSFUL);
			return true;
		} catch (Exception e) {
			this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
			logger.error("executeLoad failure", e);
			return false;
		} finally {
			try {
				if (extendtablename != null) {
					dropExtendTable(jdbc, extendtablename);
				}
				jdbc.execute("commit");

			} catch (Exception x) {
				logger.error("executeLoad failure", x);
				this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
				return false;
			}

		}

	}

	/*
	 * 1196243745000
	 */
	public static void main(String argv[]) {
		// try {
		// Class.forName("org.postgresql.Driver");
		// } catch (ClassNotFoundException e) {
		// System.err.println("Can not find the DB Driver: ");
		// return;
		// }
		// LoadCommand lc = new LoadCommand();
		// lc.setEventtype(0x1404);
		// lc.setCycle("mon");
		// lc.setTimestamp("2012-04-17 12:13:32.000");
		// lc.setFilepath("/home/gpadmin/test/wgtest/test.file");
		// DataLoader sl = new PostgreSQLoader(0);
		// sl.init();
		// sl.load(lc);
		// System.out.println(lc.getLoadresultstr());
	}

}
