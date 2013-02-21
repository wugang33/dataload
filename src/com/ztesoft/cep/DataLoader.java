package com.ztesoft.cep;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ztesoft.cep.model.LoadTask;

public class DataLoader extends Thread {
	public List<LoadTask> tasks = null;
	static Logger logger = Logger.getLogger(DataLoader.class.getName());
	public String key = "";
	public int id;
	public boolean isalive = true;
	JdbcTemplate jdbc = null;

	public DataLoader() {
		// this.initLog();
	}

	public void register(String key, List<LoadTask> tasks, JdbcTemplate jdbc) {
		this.key = key;
		this.tasks = tasks;
		this.jdbc = jdbc;
		logger.info("register new tasks tablename[" + key + "] files[" + tasks.size() + "]");
	}

	@Override
	public void run() {
		while (isalive) {
			try {
				LoadMaster.instance().registerWorker(this);
				if (this.key.equals("")) {
					Thread.sleep(100);
					continue;
				}
				// not get the task
				while (this.tasks != null) {
					for (LoadTask task : this.tasks) {
						long start_tick = System.currentTimeMillis();
						task.setTaskLastExecStartTime(new Date());
						this.load(task);
						task.setTaskLastExecEndTime(new Date());
						long end_tick = System.currentTimeMillis();
						if (task.getState() == LoadTask.TASKSTATE.SUCCESSFUL) {
							logger.info("load spend[" + (end_tick - start_tick) / 1000 + "] table["
									+ this.key + "] file[" + task.getDestFilePath() + "] size["
									+ task.getDestFileSize() + "] successful...");
						}
						LoadMaster.instance().archive(task);

					}
					this.tasks = LoadMaster.instance().requestTask(this.key, this);
				}
				LoadMaster.instance().unregisterWorker(this.key, jdbc);
				logger.info("unregister key[" + key + "]");
				jdbc = null;
				this.key = "";
				this.tasks = null;
			} catch (Exception e) {
				// e.printStackTrace();
				logger.error("is must not found in here run exception ", e);
			}
		}
	}

	public void load(LoadTask task) {

	}

	public boolean init(int id) {
		this.id = id;
		return true;
	}

	public boolean startup() {
		this.start();
		return true;
	}

	public boolean shutdown() {
		this.isalive = false;
		return true;
	}

	@Override
	public void destroy() {
		this.isalive = false;
		try {
			this.join();
		} catch (InterruptedException e) {
			logger.error("destroy failure", e);
		}
		return;
	}

	public void setCmdResult(LoadTask task, LoadTask.TASKSTATE result) {
		task.setState(result);
	}

	public static void main(String[] argc) {
		// PropertyConfigurator.configure(System.getProperty("user.dir") +
		// File.separator + "conf" + File.separator + "log4j.properties");
		// DataLoader d = new DataLoader();
		// d.initLog();
		// String[] s = d.url_log.split("\\:");
		//
		// System.out.println(s[1]);
		// while (true) {
		// // Connection conn = d.getConn();
		// Connection conn = d.getConnLog();
		// System.out.println(conn);
		// try {
		// conn.close();
		// } catch (SQLException e)
		// e.printStackTrace();
		// }
		// }
		// d.init();
	}

}
