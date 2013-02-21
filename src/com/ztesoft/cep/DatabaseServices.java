package com.ztesoft.cep;

import java.sql.Connection;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.LoadTask;
import com.ztesoft.cep.utils.DBUtils;

public class DatabaseServices implements Runnable {
	static Logger logger = Logger.getLogger(DatabaseServices.class.getName());
	public boolean isalive = true;

	public abstract class DatabaseCallBack {
		public long lastCalledTick = 0;
		public long callGeneratedTick = System.currentTimeMillis();
		public long callRateTick = 1000;
		private Object[] obj = null;

		public Object[] getObj() {
			return obj;
		}

		public void setObj(Object[] obj) {
			this.obj = obj;
		}

		public abstract void CallBackHandler(JdbcTemplate jdbc, Object[] params);
	};

	public xxx UpdateLoadConfigAndCreateAllTableCallBack extends DatabaseCallBack {

		@Override
		public void CallBackHandler(JdbcTemplate jdbc, Object[] params) {
			long start_updataconfig = System.currentTimeMillis();
			Map<Integer, LoadConfig> configs = LoadMaster.instance().config.updateLoadConfig(jdbc);
			int lastCreateTadayTableDate = ((Integer) params[0]).intValue();
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(new Date());
			int currentDateofMouth = calendar.get(Calendar.DAY_OF_MONTH);
			if (currentDateofMouth == lastCreateTadayTableDate) {
				if (configs != null && configs.isEmpty() == false) {
					logger.info("found new Config size is " + configs.size());
					DBUtils.createTodayTables(jdbc, configs);
				}
			} else {
				DBUtils.createTodayTables(jdbc, LoadMaster.instance().config.loadConfigMap);
				lastCreateTadayTableDate = currentDateofMouth;
			}
			long end_updataconfig = System.currentTimeMillis();
			if (end_updataconfig - start_updataconfig > 1000) {
				logger.info("UpdateLoadConfigAndCreateAllTableCallBack exec spend mils "
						+ (end_updataconfig - start_updataconfig));
			}
			LoadMaster.instance().dbServices.asynUpdateLoadConfigAndCreateAllTable(
					lastCreateTadayTableDate, start_updataconfig);
		}

	}

	public class TaskSearchFromTableAndUpdateStateCallBack extends DatabaseCallBack {

		@Override
		public void CallBackHandler(JdbcTemplate jdbc, Object[] params) {
			long start_updataconfig = System.currentTimeMillis();
			LoadTask.TASKSTATE src = (LoadTask.TASKSTATE) params[0];
			LoadTask.TASKSTATE dst = (LoadTask.TASKSTATE) params[1];
			List<LoadTask> tasks = LoadMaster.instance().taskSearchFromTable(jdbc, src);
			if (tasks != null && tasks.isEmpty() == false) {
				logger.info("get task from table size " + tasks.size());
				for (LoadTask task : tasks) {
					task.setState(dst.ordinal());
					task.setStateInfo(LoadTask.STATEINFOSTR[dst.ordinal()]);
				}
				if (DBUtils.archiveTaskStateBatch(jdbc, tasks) == false) {
					logger.error("lastSearchFromTable archiveTaskStateBatch failure ");
				}
			}
			long end_updataconfig = System.currentTimeMillis();
			if (end_updataconfig - start_updataconfig > 1000) {
				logger.info("TaskSearchFromTableAndUpdateStateCallBack exec spend mils "
						+ (end_updataconfig - start_updataconfig));
			}
			LoadMaster.instance().dbServices.asynTaskSearchFromTableAndUpdateState(src, dst,
					start_updataconfig);
		}
	}

	public class TaskArchiveCallBack extends DatabaseCallBack {

		@Override
		public void CallBackHandler(JdbcTemplate jdbc, Object[] params) {
			long start_updataconfig = System.currentTimeMillis();
			@SuppressWarnings("unchecked")
			List<LoadTask> newTasks = (List<LoadTask>) params[0];
			@SuppressWarnings("unchecked")
			List<LoadTask> oldTasks = (List<LoadTask>) params[1];
			if (newTasks.isEmpty() == false) {
				DBUtils.insertTasks(jdbc, newTasks);
				logger.info("insert to interface table "
						+ LoadMaster.instance().config.interfacetablename + " tasks size is "
						+ newTasks.size());
			}
			if (oldTasks.isEmpty() == false) {
				DBUtils.archiveTaskStateBatch(jdbc, oldTasks);
				logger.info("update state to interface table "
						+ LoadMaster.instance().config.interfacetablename + " tasks size is "
						+ oldTasks.size());
			}

			long end_updataconfig = System.currentTimeMillis();
			if (end_updataconfig - start_updataconfig > 1000) {
				logger.info("TaskArchiveCallBack exec spend mils "
						+ (end_updataconfig - start_updataconfig));
			}
		}
	}

	private List<DatabaseCallBack> requests = new LinkedList<DatabaseCallBack>();
	private Integer requests_lock = new Integer(0);

	private JdbcTemplate jdbc = null;

	public boolean init() {
		try {
			if (jdbc == null) {
				Connection conn = LoadMaster.getConn();
				DataSource dataSource = new SingleConnectionDataSource(conn, false);
				jdbc = new JdbcTemplate(dataSource);
			}
		} catch (Exception e) {
			logger.error("init failure ", e);
			return false;
		}
		return true;
	}

	// synchronization call
	public void synUpdateLoadConfig() {
		synchronized (jdbc) {
			LoadMaster.instance().config.updateLoadConfig(jdbc);
		}
	}

	public void asynUpdateLoadConfigAndCreateAllTable(int lastCreateTadayTableDate,
			long lastCallTick) {
		UpdateLoadConfigAndCreateAllTableCallBack callback = new UpdateLoadConfigAndCreateAllTableCallBack();
		callback.lastCalledTick = lastCallTick;
		callback.callRateTick = 10000;
		Object[] params = new Object[] { new Integer(lastCreateTadayTableDate) };
		callback.setObj(params);
		synchronized (requests_lock) {
			this.requests.add(callback);
		}
	}

	public List<LoadTask> synTaskSearchFromTable(LoadTask.TASKSTATE state) {
		synchronized (jdbc) {
			return LoadMaster.instance().taskSearchFromTable(jdbc, state);
		}
	}

	public void asynTaskSearchFromTableAndUpdateState(LoadTask.TASKSTATE src,
			LoadTask.TASKSTATE dst, long lastCallTick) {
		TaskSearchFromTableAndUpdateStateCallBack callback = new TaskSearchFromTableAndUpdateStateCallBack();
		callback.lastCalledTick = lastCallTick;
		callback.callRateTick = 10000;
		Object[] params = new Object[] { src, dst };
		callback.setObj(params);
		synchronized (requests_lock) {
			this.requests.add(callback);
		}
	}

	public void asynTaskArchive(List<LoadTask> needInsertTask, List<LoadTask> needUpdateTask) {
		TaskArchiveCallBack callback = new TaskArchiveCallBack();
		Object[] params = new Object[] { needInsertTask, needUpdateTask };
		callback.setObj(params);
		synchronized (requests_lock) {
			this.requests.add(callback);
		}
	}

	public void safeSleep(long mils) {
		try {
			Thread.sleep(mils);
		} catch (InterruptedException e) {
			logger.error(e);
		}
	}

	@Override
	public void run() {
		List<DatabaseCallBack> requests_temp = new LinkedList<DatabaseServices.DatabaseCallBack>();
		while (isalive) {
			try {
				if (this.requests.isEmpty()) {
					this.safeSleep(1000);
					continue;
				}
				synchronized (requests_lock) {
					requests_temp.addAll(this.requests);
					this.requests.clear();
				}
				int processedNum = 0;
				for (DatabaseCallBack callback : requests_temp) {
					if (callback.lastCalledTick + callback.callRateTick < System
							.currentTimeMillis()) {
						processedNum++;
						callback.CallBackHandler(jdbc, callback.getObj());
						logger.info("process a Database Services after "
								+ callback.getClass().getName());
					} else {
						synchronized (requests_lock) {
							requests.add(callback);
						}
					}
				}
				requests_temp.clear();
				if (processedNum == 0) {
					this.safeSleep(1000);
				}
			} catch (Exception e) {
				logger.error("", e);
			}
		}
		logger.info("out");
	}
}
