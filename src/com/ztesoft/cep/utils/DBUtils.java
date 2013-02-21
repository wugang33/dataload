package com.ztesoft.cep.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import com.ztesoft.cep.LoadMaster;
import com.ztesoft.cep.config.LoadTaskExtractor;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.LoadTask;

public class DBUtils {
	static Logger logger = Logger.getLogger(DBUtils.class.getName());
	static long maxId = -1;

	public static void setMaxId(long currentId) {
		maxId = currentId;
	}

	public static void closeConn(Connection conn) {
		try {
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			logger.info("", e);
		}
	}

	/*
	 * add toLowerCase();
	 */

	public static void createTable(JdbcTemplate jdbc, LoadTask task) {
		String tableSchema = new String(task.getConfig().getTableSchemaString());
		tableSchema = tableSchema.toLowerCase().replace(
				task.getConfig().getTableSchema().tablename.toLowerCase(),
				task.getDestTablename().toLowerCase());
		jdbc.execute(tableSchema);
		jdbc.execute("commit");
		logger.info(tableSchema);
	}

	public static boolean createInterfaceTable(JdbcTemplate jdbc, String tablename) {
		try {
			String tableSchema = "create table " + tablename + "(\n"
					+ "id                  		 bigint                         not null,\n"
					+ "eventtype           		 int                            not null,\n"
					+ "contentTimestamp   		 datetime                       not null,\n"
					+ "destFilePath       		 varchar(256)                   not null,\n"
					+ "destFileSize      		 bigint                         null,\n"
					+ "state               		 tinyint                        not null,\n"
					+ "stateInfo          		 varchar(8000)                  null,\n"
					+ "taskGenTime        		 datetime                       null,\n"
					+ "taskLastExecStartTime     datetime                       null,\n"
					+ "taskLastExecEndTime       datetime                       null,\n"
					+ "destTablename        	 varchar(256)                   null,\n"
					+ "loadSchema	        	 varchar(8000)                  null,\n"
					+ "constraint PK_CEP_DATALOAD_INTERFACE primary key (id))\n";

			jdbc.execute(tableSchema);
			jdbc.execute("commit");
			logger.info(tableSchema);
			return true;
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
	}

	public static boolean createConfigTable(JdbcTemplate jdbc, String tablename) {
		try {
			String tableSchema = "create table " + tablename + "(\n"
					+ "eventtype            int                            not null,\n"
					+ "cycle                varchar(256)                   null,\n"
					+ "tableschema          varchar(32767)                 null,\n"
					+ "partgranularity      int                        null,\n"
					+ "state                int                        null,\n"
					+ "constraint PK_CEP_DATALOAD_SERVICES primary key (eventtype))\n";
			jdbc.execute(tableSchema);
			jdbc.execute("commit");
			logger.info(tableSchema);
			return true;
		} catch (Exception e) {
			logger.error("", e);
			return false;
		}
	}

	public static void createTableNow(JdbcTemplate jdbc, LoadConfig config) {
		String tablename = config.buildNowTableName();

		String tableSchema = new String(config.getTableSchemaString());
		tableSchema = tableSchema.toLowerCase().replace(
				config.getTableSchema().tablename.toLowerCase(), tablename.toLowerCase());
		jdbc.execute(tableSchema);
		jdbc.execute("commit");
		logger.info("The create table script [ " + tableSchema + " ]");
	}

	public static boolean Exists(JdbcTemplate jdbc, String tablename) {
		try {
			jdbc.queryForInt("select count(1) from " + tablename + " where 1=0");
			jdbc.execute("commit");
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	public static List<LoadTask> getTaskFromInterfaceTable(JdbcTemplate jdbc, int state) {
		String queryString = "select id,eventtype,contentTimestamp,destFilePath,destFileSize,state,taskGenTime,taskLastExecStartTime,taskLastExecEndTime,destTablename "
				+ "from  " + LoadMaster.instance().config.interfacetablename + " where state=?";
		List<LoadTask> task = jdbc.query(queryString, new LoadTaskExtractor(),
				new Object[] { state });
		jdbc.execute("commit");
		return task;
	}

	/*
	 * public static List<LoadTask> getTaskFromTableAndUpdateState( JdbcTemplate
	 * jdbc, int searchState, int changeToState) { List<LoadTask> tasks =
	 * getTaskFromInterfaceTable(jdbc, searchState); if (tasks == null) return
	 * null; for (LoadTask task : tasks) { task.setState(changeToState);
	 * updateLoadTaskState(jdbc, task); } updateListLoadTaskState(jdbc, tasks);
	 * return tasks; }
	 * 
	 * public static boolean updateListLoadTaskState(JdbcTemplate jdbc,
	 * List<LoadTask> tasks) { String updateSQL = "update " +
	 * LoadMaster.instance().config.getInterfacetablename() +
	 * " set state=?,stateinfo=? where "; for (int i = 0; i < tasks.size(); i++)
	 * { if (i == 0) { updateSQL += " id=" + tasks.get(i).getId(); } else {
	 * updateSQL += " or id=" + tasks.get(i).getId(); } } jdbc.update(updateSQL,
	 * new Object[] { tasks.get(0).getState().ordinal(),
	 * tasks.get(0).getStateInfo() }); jdbc.execute("commit"); return false; }
	 * 
	 * public static boolean updateLoadTaskState(JdbcTemplate jdbc, LoadTask
	 * task) { String updateSQL = "update " +
	 * LoadMaster.instance().config.getInterfacetablename() +
	 * " set state=?,stateinfo=? where id=?"; Object[] values = {
	 * task.getState().ordinal(), task.getStateInfo(), task.getId() };
	 * jdbc.update(updateSQL, values); jdbc.execute("commit"); return false; }
	 */
	public static boolean insertLoadTask(JdbcTemplate jdbc, LoadTask task, String tablename) {
		try {
			String insertSql = "insert into "
					+ tablename
					+ "(id,eventtype,contentTimestamp,destFilePath,destFileSize,state,stateinfo,taskGenTime,taskLastExecStarttime,taskLastExecEndTime,destTablename) values(?,?,?,?,?,?,?,?,?,?,?)";
			jdbc.update(
					insertSql,
					new Object[] { task.getId(), task.getEventtype(), task.getContentTimestamp(),
							task.getDestFilePath(), task.getDestFileSize(),
							task.getState().ordinal(), task.getStateInfo(), task.getTaskGenTime(),
							task.getTaskLastExecStartTime(), task.getTaskLastExecEndTime(),
							task.getDestTablename() });
			jdbc.execute("commit");
			return true;
		} catch (Exception e) {
			logger.error("insert to table[" + tablename + "] failure", e);
		}
		return false;
	}

	// test xqh

	public static boolean insertLoadTaskBatch(JdbcTemplate jdbc, List<LoadTask> tasks,
			String tablename) {
		final List<LoadTask> loadTaskList = tasks;
		try {
			String insertSql = "insert into "
					+ tablename
					+ "(id,eventtype,contentTimestamp,destFilePath,destFileSize,state,stateinfo,taskGenTime,taskLastExecStarttime,taskLastExecEndTime,destTablename) values(?,?,?,?,?,?,?,?,?,?,?)";
			// add change
			final String tablename_inner = tablename;
			jdbc.batchUpdate(insertSql, new BatchPreparedStatementSetter() {
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					// TODO Auto-generated method stub
					long id = loadTaskList.get(i).getId();
					int eventtype = loadTaskList.get(i).getEventtype();
					java.sql.Date contentTimestamp = new java.sql.Date(loadTaskList.get(i)
							.getContentTimestamp().getTime());
					String destFilePath = loadTaskList.get(i).getDestFilePath();
					long destFileSize = loadTaskList.get(i).getDestFileSize();
					int state = loadTaskList.get(i).getState().ordinal();
					String stateinfo = loadTaskList.get(i).getStateInfo();
					java.sql.Date taskGenTime = new java.sql.Date(loadTaskList.get(i)
							.getTaskGenTime().getTime());
					java.sql.Date taskLastExecStarttime = new java.sql.Date(loadTaskList.get(i)
							.getTaskLastExecStartTime().getTime());
					java.sql.Date taskLastExecEndTime = new java.sql.Date(loadTaskList.get(i)
							.getTaskLastExecEndTime().getTime());
					String destTablename = loadTaskList.get(i).getDestTablename();
					ps.setLong(1, id);
					ps.setInt(2, eventtype);
					ps.setDate(3, contentTimestamp);
					ps.setString(4, destFilePath);
					ps.setLong(5, destFileSize);
					ps.setInt(6, state);
					ps.setString(7, stateinfo);
					ps.setDate(8, taskGenTime);
					ps.setDate(9, taskLastExecStarttime);
					ps.setDate(10, taskLastExecEndTime);
					ps.setString(11, destTablename);
					String sql = "insert into "
							+ tablename_inner
							+ "(id,eventtype,contentTimestamp,destFilePath,destFileSize,state,stateinfo,taskGenTime,taskLastExecStarttime,taskLastExecEndTime,destTablename) values("
							+ id + "," + eventtype + "," + contentTimestamp + "," + destFilePath
							+ "," + destFileSize + "," + state + "," + stateinfo + ","
							+ taskGenTime + ",null,null," + destTablename;
					logger.debug("The insert sql is [ " + sql + " ]");
				}

				@Override
				public int getBatchSize() {
					// TODO Auto-generated method stub
					return loadTaskList.size();
				}
			});
			logger.debug("The insert sql is " + insertSql);
			jdbc.execute("commit");
			return true;
		} catch (Exception e) {
			logger.error("insert to table[" + tablename + "] failure", e);
		}
		return false;
	}

	public static long getMaxId(JdbcTemplate jdbc, String tablename, String idField) {
		if (DBUtils.maxId == -1) {
			try {
				long id = jdbc.queryForLong("select max(" + idField + ") from " + tablename);
				jdbc.execute("commit");
				DBUtils.maxId = id;
			} catch (Exception e) {
				logger.error(e);
				return -1;
			}
		}
		return DBUtils.maxId;

	}

	// may return false when someothers lock the interface table and then may
	// return and try again
	public static boolean archiveTaskState(JdbcTemplate jdbc, LoadTask task) {
		try {
			String updateString = "update "
					+ LoadMaster.instance().config.getInterfacetablename()
					+ " set state=?,stateinfo=?,taskLastExecStartTime=?,taskLastExecEndTime=? where id=?";
			Object[] objs = { task.getState().ordinal(), task.getStateInfo(),
					task.getTaskLastExecStartTime(), task.getTaskLastExecEndTime(), task.getId() };
			jdbc.update(updateString, objs);
			jdbc.execute("commit");
			return true;
		} catch (Exception e) {
			logger.error("can't archive task state", e);
			return false;
		}
	}

	public static List<LoadTask> archiveTaskStateSuccess(JdbcTemplate jdbc, List<LoadTask> tasks) {
		List<LoadTask> unsuccessfulTask = null;
		try {
			String updateString = "update "
					+ LoadMaster.instance().config.getInterfacetablename()
					+ " set state=?,stateinfo=?,taskLastExecStartTime=?,taskLastExecEndTime=? where ";
			for (int i = 0; i < tasks.size(); i++) {
				if (i == tasks.size() - 1) {
					updateString = updateString + "id=" + tasks.get(i).getId();
				} else {
					updateString = updateString + "id=" + tasks.get(i).getId() + " or ";
				}
			}
			Object[] objs = { tasks.get(0).getState().ordinal(), tasks.get(0).getStateInfo(),
					tasks.get(0).getTaskLastExecStartTime(), tasks.get(0).getTaskLastExecEndTime() };
			logger.info(updateString);
			int fetchnum = jdbc.update(updateString, objs);
			if (fetchnum != tasks.size()) {
				logger.info("fetch num " + fetchnum + "not equal tasks num " + tasks.size());
			}
			jdbc.execute("commit");
			LoadMaster.instance().arhiveThread.pushAll(tasks);
		} catch (Exception e) {
			logger.error("archiveTaskStateSuccess failure due to exception", e);
			if (DBUtils.archiveTaskStateBatch(jdbc, tasks) == false) {
				for (LoadTask task : tasks) {
					if (DBUtils.archiveTaskState(jdbc, task) == false) {
						if (unsuccessfulTask == null)
							unsuccessfulTask = new LinkedList<LoadTask>();
						logger.error("archive task state failure and will try it latter");
						unsuccessfulTask.add(task);
					} else {
						LoadMaster.instance().arhiveThread.push(task);
					}
				}
			}
		}
		return unsuccessfulTask;
	}

	public static List<LoadTask> archiveTaskStateError(JdbcTemplate jdbc, List<LoadTask> tasks) {
		List<LoadTask> unsuccessfulTask = null;
		if (DBUtils.archiveTaskStateBatch(jdbc, tasks) == false) {
			for (LoadTask task : tasks) {
				if (DBUtils.archiveTaskState(jdbc, task) == false) {
					if (unsuccessfulTask == null)
						unsuccessfulTask = new LinkedList<LoadTask>();
					logger.error("archive task state failure and will try it latter");
					unsuccessfulTask.add(task);
				} else {
					LoadMaster.instance().arhiveThread.push(task);
				}
			}
		}
		return unsuccessfulTask;
	}

	public static boolean insertTasks(JdbcTemplate jdbc, List<LoadTask> tasks) {
		if (DBUtils.insertLoadTaskBatch(jdbc, tasks,
				LoadMaster.instance().config.interfacetablename) == false) {
			logger.warn("insertLoadTaskBatch exec failure ");
			boolean ret = true;
			for (LoadTask task : tasks) {
				if (DBUtils.insertLoadTask(jdbc, task,
						LoadMaster.instance().config.interfacetablename) == false) {
					logger.error("archive task state failure and will try it latter");
					ret = false;
				}
			}
			return ret;
		}
		return true;
	}

	// add by xqh
	public static boolean archiveTaskStateBatch(JdbcTemplate jdbc, List<LoadTask> tasks) {
		logger.debug("archiveTaskStateBatch : update batch start time ");
		final List<LoadTask> loadTaskList = tasks;
		try {
			String updateSql = "update "
					+ LoadMaster.instance().config.getInterfacetablename()
					+ " set state=?,stateinfo=?,taskLastExecStartTime=?,taskLastExecEndTime=? where id=?";
			jdbc.batchUpdate(updateSql, new BatchPreparedStatementSetter() {
				@Override
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					// TODO Auto-generated method stub
					int state = loadTaskList.get(i).getState().ordinal();
					String stateinfo = loadTaskList.get(i).getStateInfo();
					java.sql.Date taskLastExecStarttime = null;
					if (loadTaskList.get(i).getTaskLastExecStartTime() != null) {
						taskLastExecStarttime = new java.sql.Date(loadTaskList.get(i)
								.getTaskLastExecStartTime().getTime());
					}
					java.sql.Date taskLastExecEndTime = null;
					if (loadTaskList.get(i).getTaskLastExecEndTime() != null) {
						taskLastExecEndTime = new java.sql.Date(loadTaskList.get(i)
								.getTaskLastExecEndTime().getTime());
					}
					long id = loadTaskList.get(i).getId();
					ps.setInt(1, state);
					ps.setString(2, stateinfo);
					ps.setDate(3, taskLastExecStarttime);
					ps.setDate(4, taskLastExecEndTime);
					ps.setLong(5, id);
				}

				@Override
				public int getBatchSize() {
					// TODO Auto-generated method stub
					return loadTaskList.size();
				}
			});
			jdbc.execute("commit");
			logger.debug("archiveTaskStateBatch : update batch end time ");
			return true;
		} catch (Exception e) {
			logger.error("can't archive task state", e);
		}

		return false;
	}

	public static void createTodayTables(JdbcTemplate jdbc, Map<Integer, LoadConfig> configs) {
		for (Integer integer : configs.keySet()) {
			LoadConfig config = configs.get(integer);
			String tablename = config.buildNowTableName();
			if (Exists(jdbc, tablename)) {
				logger.debug(tablename + " is already exists");
				continue;
			}
			createTableNow(jdbc, config);
		}
	}
}
