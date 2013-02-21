package com.ztesoft.cep;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import java.util.HashSet;
import javax.sql.DataSource;
import com.ztesoft.cep.config.LoadMasterConfig;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.LoadTask;
import com.ztesoft.cep.utils.DBUtils;
import com.ztesoft.cep.utils.FileUtils;
import com.ztesoft.cep.utils.MyFileFilter;

public class LoadMaster extends Thread {

	public List<DataLoader> dls = new ArrayList<DataLoader>();
	// 注册了的表名放在这个set里面 用来表示那些表名已经被注册了
	public Set<String> registed_worker = new HashSet<String>();
	// 用来保护registed_worker
	public Integer registerd_workder_lock = new Integer(0);

	public static LoadMaster loadmaster = new LoadMaster();

	public DatabaseServices dbServices = new DatabaseServices();

	// key 表名 value 应该是很多的LoadCommand 很多的command
	public Map<String, List<LoadTask>> undispatched = new HashMap<String, List<LoadTask>>();
	public Integer undispatched_lock = new Integer(0);

	public List<LoadTask> unarchived = new LinkedList<LoadTask>();
	public Integer unarchived_lock = new Integer(0);

	public Integer interface_table_alter_lock = new Integer(0);

	static Logger logger = Logger.getLogger(LoadMaster.class.getName());
	public ArchiveThread arhiveThread = null;

	public LoadMasterConfig config;
	boolean isalive = true;
	JdbcTemplate jdbc = null;

	public static DataSource dataSource = null;

	private LoadMaster() {
	}

	public static Connection getConn() {
		if (dataSource == null) {
			logger.error("dataSource is null");
			return null;
		}
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("can't get Connection ", e);
			return null;
		}
	}

	public static LoadMaster instance() {
		return loadmaster;
	}

	public void add2undispatch(String key, LoadTask task) {
		synchronized (this.undispatched_lock) {
			List<LoadTask> tasks = this.undispatched.get(key);
			if (tasks != null) {
				tasks.add(task);
				return;
			} else {
				tasks = new ArrayList<LoadTask>();
				tasks.add(task);
				this.undispatched.put(key, tasks);
			}
		}
	}

	public List<LoadTask> requestTask(String key, DataLoader loader) {
		synchronized (this.undispatched_lock) {
			if (this.undispatched.containsKey(key)) {
				List<LoadTask> tasks = this.undispatched.get(key);
				this.undispatched.remove(key);
				logger.info("loader[" + loader.getId() + "] request task[" + key + "] tasks size["
						+ tasks.size() + "]");
				return tasks;
			}
		}
		return null;
	}

	public void registerWorker(DataLoader loader) {
		// you must get two locks
		List<LoadTask> tasks = null;
		String tablename = null;
		synchronized (this.undispatched_lock) {
			synchronized (this.registerd_workder_lock) {
				for (String key : this.undispatched.keySet()) {
					if (this.registed_worker.contains(key)) {
						continue;
					}
					tasks = this.undispatched.get(key);
					tablename = key;
					this.undispatched.remove(key);
					if (!this.registed_worker.add(key)) {
						logger.error("must not found in here");
					}
					break;
				}
			}
		}
		if (tasks != null && tablename != null && tasks.isEmpty() == false) {
			Connection conn = LoadMaster.getConn();
			DataSource dataSource = new SingleConnectionDataSource(conn, false);
			JdbcTemplate jdbc4load = new JdbcTemplate(dataSource);
			try {
				long exist_start = System.currentTimeMillis();
				if (DBUtils.Exists(jdbc4load, tablename) == false) {
					DBUtils.createTable(jdbc4load, tasks.get(0));
				}
				long exist_end = System.currentTimeMillis();
				logger.info("exist spend " + (exist_end - exist_start));
			} catch (Exception e) {
				logger.error(e);
			}
			logger.info("loader[" + loader.getId() + "] register new type[" + tablename + "]");
			loader.register(tablename, tasks, jdbc4load);
		} else {
			if (tasks != null && tablename != null) {
				logger.error("tasks is empty size[" + tasks.isEmpty() + "]");
			}
		}
	}

	public void unregisterWorker(String key, JdbcTemplate jdbcTemplate) {
		synchronized (this.registerd_workder_lock) {
			if (this.registed_worker.contains(key) == false) {
				logger.error("must not found in here");
			}
			this.registed_worker.remove(key);
		}
		try {
			DBUtils.closeConn(jdbcTemplate.getDataSource().getConnection());
		} catch (SQLException e) {
			logger.error("unregisterWorker", e);
		}
	}

	// add by xqh 2013-01-24
	public List<LoadTask> taskSearchFromTable(JdbcTemplate jdbc, LoadTask.TASKSTATE state) {
		List<LoadTask> tasksList = DBUtils.getTaskFromInterfaceTable(jdbc, state.ordinal());
		if (tasksList != null && tasksList.size() > 0) {
			for (LoadTask task : tasksList) {
				LoadConfig config = this.config.loadConfigMap.get(task.getEventtype());
				if (config == null) {
					logger.error("taskSearchFromTable Can't get config for type["
							+ task.getEventtype() + "] from table[" + this.config.configtablename
							+ "]");
					continue;
				}
				task.setConfig(config);
				task.setNewTask(false);
				LoadMaster.instance().add2undispatch(task.getDestTablename(), task);
			}
		}
		return tasksList;
	}

	public void reloadOpeartingTask() {
		this.getTask(config.getCommdoing());
	}

	public void getTask(String path) {
		long listfile_start = System.currentTimeMillis();
		File execpath = new File(path);
		File filelist[] = execpath.listFiles(new MyFileFilter(this.config.getFileregex()));
		long listfile_end = System.currentTimeMillis();
		if (listfile_end - listfile_start > 500) {
			logger.info("list file speen " + (listfile_end - listfile_start));
		}
		if (filelist == null || filelist.length == 0) {
			return;
		}
		listfile_start = System.currentTimeMillis();
		long id = DBUtils.getMaxId(jdbc, this.config.interfacetablename, "id");
		if (id == -1) {
			logger.error("get max id from " + this.config.interfacetablename + "failure");
			return;
		}
		for (File commandfile : filelist) {
			logger.debug("start read the command file" + commandfile.toString());
			if (!commandfile.isFile())
				continue;
			List<LoadTask> result = FileUtils.getCommandFromFile(commandfile, ",", 6);
			// some thing must be error rename the file 2 error
			if (result == null || result.isEmpty()) {
				logger.info("split file[" + commandfile.toString() + "]");
				continue;
			}
			// boolean insertSuccessful = false;
			for (LoadTask task : result) {
				task.setCommadnFile(commandfile);
				LoadConfig config = this.config.loadConfigMap.get(task.getEventtype());
				if (config == null) {
					logger.error("can't get config for type[" + task.getEventtype()
							+ "] from table[" + this.config.configtablename + "]");
					continue;
				}
				String key = config.buildTableName(task.getContentTimestamp());

				if (key == "") {
					logger.error("build Table Name Error");
					continue;
				}
				if (FileUtils.taskCmdMove(task, this.config.commdoing) == false) {
					logger.error("mv cmd to doing failure");
					continue;
				}
				id++;
				task.setId(id);
				task.setDestFileSize(FileUtils.getFilesize(task.getDestFilePath()));
				task.setDestTablename(key);
				task.setTaskGenTime(new Date());
				task.setState(LoadTask.TASKSTATE.OPERATING);
				task.setConfig(config);
				LoadMaster.instance().add2undispatch(task.getDestTablename(), task);
			}
		}
		DBUtils.setMaxId(id);
	}

	List<String> directorylist(String path) {
		File input_path = new File(path);
		List<String> s = new ArrayList<String>();
		if (input_path.isDirectory()) {
			File fs[] = input_path.listFiles();
			if (fs == null) {
				return s;
			}
			for (File tmpf : fs) {
				if (tmpf.isDirectory()) {
					s.add(tmpf.getAbsolutePath());
					s.addAll(this.directorylist(tmpf.getAbsolutePath()));
				}
			}
		}
		return s;
	}

	void taskSearchFromPath() {
		getTask(config.getCommexec());
		List<String> pathlist = this.directorylist(config.getCommexec());
		for (String path1 : pathlist) {
			getTask(path1);
		}
	}

	boolean init_worker() {
		for (int i = 0; i < config.getLoad_works(); i++) {
			try {
				DataLoader dl = (DataLoader) ClassLoader.getSystemClassLoader()
						.loadClass(config.getLoadclass()).newInstance();
				dl.init(i);
				dls.add(dl);
				dl.startup();
			} catch (ClassNotFoundException e) {
				logger.error(e.getMessage(), e);
				return false;
			} catch (InstantiationException e) {
				logger.error(e.getMessage(), e);
				return false;
			} catch (IllegalAccessException e) {
				logger.error(e.getMessage(), e);
				return false;
			}
		}
		return true;
	}

	void startDBServiceThread() {
		Thread t = new Thread(this.dbServices);
		t.start();
	}

	void startArchiveThread() {
		this.arhiveThread = new ArchiveThread(this);
		Thread t = new Thread(this.arhiveThread);
		t.start();
	}

	boolean init_global_config() {
		config = new LoadMasterConfig();
		if (config.initConfig() == false) {
			logger.error("init config fail!!! exit program");
			return false;
		}
		return true;
	}

	public void archive(LoadTask task) {
		this.arhiveThread.push(task);
		logger.info("add load  task state[" + task.getState().ordinal()
				+ "] to unarchived quene,the size is " + unarchived.size());
	}

	public boolean initJdbc() {
		try {
			Connection conn = LoadMaster.getConn();
			DataSource dataSource = new SingleConnectionDataSource(conn, false);
			jdbc = new JdbcTemplate(dataSource);
			return true;
		} catch (Exception e) {
			logger.error("initJdbc Error", e);
			return false;
		}
	}

	@Override
	public void run() {
		try {
			// init config
			if (init_global_config() == false) {
				return;
			}
			// init DataLoader
			if (this.init_worker() == false) {
				return;
			}
			// init database connection
			if (this.initJdbc() == false) {
				return;
			}
			if (this.dbServices.init() == false) {
				return;
			}
			if (DBUtils.Exists(jdbc, this.config.interfacetablename) == false) {
				if (DBUtils.createInterfaceTable(jdbc, this.config.interfacetablename) == false) {
					return;
				}
			}
			if (DBUtils.Exists(jdbc, this.config.configtablename) == false) {
				if (DBUtils.createConfigTable(jdbc, this.config.configtablename) == false) {
					return;
				}
			}
			// start the archive thread
			this.startArchiveThread();
			this.startDBServiceThread();
			// get the configuration from cep_dataload_services
			this.dbServices.synUpdateLoadConfig();
			// this.config.updateLoadConfig(jdbc);
			/*
			 * author:xqh date:2013-01-24
			 */
			this.reloadOpeartingTask();
			this.dbServices.synTaskSearchFromTable(LoadTask.TASKSTATE.OPERATING);
			// this.taskSearchFromTable(jdbc, LoadTask.TASKSTATE.OPERATING);
			this.dbServices.asynTaskSearchFromTableAndUpdateState(LoadTask.TASKSTATE.NEW,
					LoadTask.TASKSTATE.OPERATING, 0);
			this.dbServices.asynUpdateLoadConfigAndCreateAllTable(0, 0);

			long lastSearchTick = 0;
			long lastLogTick = 0;
			// main loops
			while (isalive) {
				// main loops
				// 1 tick means 0.01 second
				// search the new task from path
				// 0.2s
				long currentTick = System.currentTimeMillis();
				try {
					if (currentTick - lastSearchTick > 200) {
						lastSearchTick = currentTick;
						long start_search = System.currentTimeMillis();
						this.taskSearchFromPath();
						long end_search = System.currentTimeMillis();
						if (end_search - start_search > 1000) {
							logger.info("lastsearch spend mills " + (end_search - start_search));
						}
					}

					if (currentTick - lastLogTick > 5000) {
						lastLogTick = currentTick;
						logger.info("Master  undispatch[" + this.undispatched.size()
								+ "] loader's num[" + this.dls.size() + "]" + " unarchive["
								+ this.unarchived.size() + "]archivedthread size["
								+ arhiveThread.comm_queue.size() + "]");
						synchronized (this.registerd_workder_lock) {
							for (Iterator<String> iter = this.registed_worker.iterator(); iter
									.hasNext();) {
								String key = iter.next();
								logger.info("registed key " + key);
							}
						}
					}

					long sleepMillis = 200 - (System.currentTimeMillis() - currentTick);
					if (sleepMillis > 0) {
						Thread.sleep(sleepMillis);
					}
				} catch (Exception e) {
					logger.error("is must not found in here run exception", e);
				}
			}
		} catch (Exception e) {
			logger.error("is must not found in here run exception " + e.getMessage(), e);
		}
	}

	public static void main(String argv[]) throws InterruptedException {

		System.out.println(System.getProperty("user.dir"));
		PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf"
				+ File.separator + "log4j.properties");
		LoadMaster l = LoadMaster.instance();
		l.run();

		// xqh test
		// l.init_global_config();
		// Connection conn = LoadMaster.getConn();
		// DataSource dataSource = new SingleConnectionDataSource(conn, false);
		// JdbcTemplate jdbc4load = new JdbcTemplate(dataSource);
		// l.initJdbc();
		// l.config.updateLoadConfig(jdbc4load);
		// l.initJdbc();
		// l.getTask("E:\\ztesoft_cep\\xdrs\\cmds\\cmcc_245");
		// l.reloadOpeartingTask();
		// Iterator it = l.undispatched.keySet().iterator();
		// for (Object o : l.undispatched.keySet()) {
		// List<LoadTask> list = l.undispatched.get(o);
		// for (LoadTask task : list) {
		// task.setTaskLastExecStartTime(new Date());
		// task.setTaskLastExecEndTime(new Date());
		// task.setStateInfo("successful...");
		// task.setState(TASKSTATE.SUCCESSFUL);
		// l.unarchived.add(task);
		// System.out.println("The task key is " + o + " eventtype is "
		// + task.getEventtype() + " The state is "
		// + task.getStateInfo());
		// }
		//
	}
}
