package com.ztesoft.cep;

import java.io.File;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;
import com.ztesoft.cep.model.LoadTask;

public class IQLoader extends DataLoader {
	public IQLoader() {

	}

	@Override
	public void load(LoadTask task) {
		executeLoad(jdbc, task);
		try {
			jdbc.execute("commit");
		} catch (Exception x) {
			logger.error("commit error", x);
		}
	}

	String getLoadStr(LoadTask task) {
		String insert_scheme = "load table " + task.getDestTablename() + "(";
		for (int i = 0; i < task.getConfig().getTableSchema().fields.size(); i++) {
			insert_scheme = insert_scheme + task.getConfig().getTableSchema().fields.get(i).name;
			if (i != (task.getConfig().getTableSchema().fields.size() - 1)) {// 不是最後一個
				insert_scheme = insert_scheme + " ',',";// name ',' ,
				insert_scheme += '\n';
			} else {// is the last one
				insert_scheme = insert_scheme + " '\\x0a' "; // age '\x0a'
			}
		}
		insert_scheme += ')';
		insert_scheme += '\n';
		insert_scheme += " from  '";
		insert_scheme += task.getDestFilePath();
		insert_scheme = insert_scheme + "'  QUOTES ON  " + " ESCAPES OFF  " + " FORMAT  ASCII   "
				+ " DELIMITED BY  ',' ";
		return insert_scheme;
	}

	boolean executeLoad(JdbcTemplate jdbc, LoadTask task) {
		try {
			String loadstr = getLoadStr(task);
			task.setLoadSchema(loadstr);
			jdbc.execute("commit");
			jdbc.execute("set temporary option ESCAPE_CHARACTER='ON'");
			logger.info(loadstr);
			jdbc.execute(loadstr);
			this.setCmdResult(task, LoadTask.TASKSTATE.SUCCESSFUL);
			return true;
		} catch (Exception e) {
			logger.error("executeLoad failure", e);

			task.setStateInfo(e.getMessage());
			this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
			return false;
		} finally {
			try {
				jdbc.execute("commit");
			} catch (Exception x) {
				task.setStateInfo(x.getMessage());
				logger.error("commit error", x);
				this.setCmdResult(task, LoadTask.TASKSTATE.ERROR);
				return false;
			}
		}

	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf"
				+ File.separator + "log4j.properties");
		// DataLoader sl = new IQLoader(0);
		// sl.init();
		// sl.getConn();
		// LoadCommand command = new LoadCommand();
		// command.setCycle("60");
		// command.setEventtype(18691);
		// command.setFilepath("F:\\dataload.log");
		// command.setTimewindow("2012-09-26 03:45:00:000");
		//
		// sl.load(command);
	}
}
