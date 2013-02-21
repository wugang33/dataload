package com.ztesoft.cep.config;

import java.io.*;
import java.util.*;
import com.ztesoft.cep.*;
import com.ztesoft.cep.model.LoadConfig;
import com.ztesoft.cep.model.TableSchema;
import com.ztesoft.cep.utils.FileUtils;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.springframework.jdbc.core.JdbcTemplate;

public class LoadMasterConfig {
	public String commexec = "exec";
	public String commdoing = "doing";

	public String splitregex = ",";
	public String loadclass = "com.ztesoft.cep.IQLoader";
	public String fileregex = ".*\\.cmd";
	public int commandfields = 6;
	public int load_works = 3;
	public String dataLoadsucc = "none";
	public String dataLoadFailure = "none";

	public String url;
	public String driver;
	public String user;
	public String password;

	public String getCommexec() {
		return commexec;
	}

	public String getCommdoing() {
		return commdoing;
	}

	public String getSplitregex() {
		return splitregex;
	}

	public String getLoadclass() {
		return loadclass;
	}

	public String getFileregex() {
		return fileregex;
	}

	public int getCommandfields() {
		return commandfields;
	}

	public int getLoad_works() {
		return load_works;
	}

	public String getDataLoadsucc() {
		return dataLoadsucc;
	}

	public String getDataLoadFailure() {
		return dataLoadFailure;
	}

	public String getUrl() {
		return url;
	}

	public String getDriver() {
		return driver;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getConfigtablename() {
		return configtablename;
	}

	public String getInterfacetablename() {
		return interfacetablename;
	}

	public String getCurrentTablename() {
		return currentTablename;
	}

	public String getTns_alias() {
		return tns_alias;
	}

	public Map<Integer, LoadConfig> getLoadConfigMap() {
		return loadConfigMap;
	}

	public static Logger getLogger() {
		return logger;
	}

	public String configtablename = "cep_dataload_service";
	public String interfacetablename = "cep_dataload_interfaces";
	public String currentTablename;
	public String tns_alias = "NTP2000_5.51";

	/*
	 * modify :2012-10-18 author:xqh content:数据入库成功和失败,日志插入的数据库属性.
	 */
	public Map<Integer, LoadConfig> loadConfigMap = new HashMap<Integer, LoadConfig>();

	@SuppressWarnings("unchecked")
	public Map<Integer, LoadConfig> updateLoadConfig(JdbcTemplate jdbc) {
		Map<Integer, LoadConfig> newConfigsMap = null;
		List<LoadConfig> newconfigs = jdbc.query(
				"select eventtype,partgranularity,tableschema,cycle from " + this.configtablename
						+ " where state=1", new LoadConfigExtractor());
		if (newconfigs == null) {
			return newConfigsMap;
		}
		for (LoadConfig config : newconfigs) {
			int eventtype = config.getEventtype();
			if (this.loadConfigMap.containsKey(eventtype)) {
				LoadConfig oldconfig = this.loadConfigMap.get(eventtype);
				if (oldconfig.equals(config)) {
					continue;
				} else {
					this.loadConfigMap.remove(eventtype);
					logger.info("eventtype[" + eventtype + "] config is change");
				}
			}
			// new config
			logger.info("new config from eventtype " + eventtype);
			TableSchema ts = new TableSchema();
			if (!ts.analyze(config.getTableSchemaString())) {
				logger.error("update LoadConfig build tableschema failure type["
						+ config.getEventtype() + "] schema[" + config.getTableSchemaString() + "]");
				continue;
			}
			if (ts.tablename == null) {
				logger.error("update LoadConfig build tableschema failure type["
						+ config.getEventtype() + "] schema[" + config.getTableSchemaString() + "]"
						+ " tablename is null]");
				continue;
			}
			config.setTableSchema(ts);
			if (this.loadConfigMap.containsKey(config.getEventtype())) {
				logger.error("update LoadConfig build config failure due to unique constraints for type "
						+ config.getEventtype());
				continue;
			}
			logger.debug("insert config type:" + config.getEventtype() + " cycle:"
					+ config.getCycle() + " tablename:" + config.getTableSchema().tablename);
			this.loadConfigMap.put(config.getEventtype(), config);
			if (newConfigsMap == null) {
				newConfigsMap = new HashMap<Integer, LoadConfig>();
			}
			newConfigsMap.put(config.getEventtype(), config);
		}
		return newConfigsMap;
	}

	static Logger logger = Logger.getLogger(LoadMasterConfig.class.getName());

	public boolean loadDataloadConfig() {
		try {
			String proFilePath = System.getProperty("user.dir") + File.separator + "conf"
					+ File.separator + "dataload.properties";
			InputStream in = null;
			in = new BufferedInputStream(new FileInputStream(proFilePath));
			ResourceBundle rb = null;
			rb = new PropertyResourceBundle(in);
			String command_exec = rb.getString("command_exec");
			if (command_exec == null) {
				logger.error("Can not find the 'command_exec' property in dataload.properties.");
			} else {
				commexec = command_exec;
				logger.info("commexec[" + command_exec + "]");
				// System.out.println("commexec[" + command_exec + "]");
			}

			String command_doing = rb.getString("command_doing");
			if (command_doing == null) {
				logger.error("Can not find the 'command_doing' property in dataload.properties.");
				// System.out
				// .println("Can not find the 'command_doing' property in dataload.properties.");
			} else {
				commdoing = command_doing;
				logger.info("commdoing[" + command_doing + "]");
				// System.out.println("commdoing[" + command_doing + "]");
			}
			if (!FileUtils.mkdirsWithAccess(this.commdoing)) {
				return false;
			}
			if (!FileUtils.mkdirsWithAccess(this.commexec)) {
				return false;
			}
			String worksStr = rb.getString("load_works");
			if (worksStr != null) {
				int works = Integer.parseInt(worksStr);
				if (works > 0 && works < 100) {
					load_works = works;
				}
			}
			logger.info("works[" + load_works + "]");
			String loadclasstr = rb.getString("load_class");
			if (loadclasstr != null) {

				this.loadclass = loadclasstr;
			}
			logger.info("load class is " + this.loadclass);

			String regex = rb.getString("fileregex");
			if (regex != null) {
				this.fileregex = regex;
			}
			logger.info("file regex is " + this.fileregex);
			// System.out.println("load class is " + this.loadclass);
			String command_action = rb.getString("data_loadsuccess");
			if (command_action != null) {
				this.dataLoadsucc = command_action.trim();
			}
			command_action = rb.getString("data_loadfailure");
			if (command_action != null) {
				this.dataLoadFailure = command_action.trim();
			}
			logger.info("date_loadsuccess[" + this.dataLoadsucc + "]");
			logger.info("date_loadfailure[" + this.dataLoadFailure + "]");
			this.configtablename = rb.getString("configtablename");
			if (this.configtablename == null) {
				logger.error("configtablename must be configured");
				return false;
			}
			this.interfacetablename = rb.getString("interfacetablename");
			if (this.interfacetablename == null) {
				logger.error("interfacetablename must be configured");
				return false;
			}
			if (this.loadclass.equals("com.ztesoft.cep.IQLoader")) {

			} else if (this.loadclass.equals("com.ztesoft.cep.OracleLoader")) {
				this.tns_alias = rb.getString("tns_alias");
				if (this.tns_alias == null) {
					logger.error("Oracle must configure  tns_alias attributes");
					return false;
				}
			} else if (this.loadclass.equals("com.ztesoft.cep.PostgreSQLoader")) {

			} else {
				logger.error("unsupport load class [" + this.loadclass + "]");
				return false;
			}
			return true;
		} catch (Exception e) {
			logger.error("dataload.properties not proper config exit program!!!", e);
			return false;
		}
	}

	public boolean loadDatabaseConfig() {
		try {
			String proFilePath = System.getProperty("user.dir") + File.separator + "conf"
					+ File.separator + "database.properties";
			InputStream in = null;
			in = new BufferedInputStream(new FileInputStream(proFilePath));
			ResourceBundle rb = null;
			rb = new PropertyResourceBundle(in);
			this.url = rb.getString("url");
			if (this.url == null) {
				logger.error("Can not find the 'url' property in database.properties.");
				// System.out
				// .println("Can not find the 'command_exec' property in database.properties.");
				return false;
			}
			this.driver = rb.getString("driver");
			if (this.driver == null) {
				logger.error("Can not find the 'driver' property in database.properties.");
				// System.out
				// .println("Can not find the 'driver' property in database.properties.");
				return false;
			}
			this.user = rb.getString("user");
			if (this.user == null) {
				logger.error("Can not find the 'user' property in database.properties.");
				// System.out
				// .println("Can not find the 'user' property in database.properties.");
				return false;
			}
			this.password = rb.getString("psw");
			if (this.password == null) {
				logger.error("Can not find the 'psw' property in database.properties.");
				// System.out
				// .println("Can not find the 'command_exec' property in database.properties.");
				return false;
			}
			try {
				if (rb.containsKey("tns_alias")) {
					this.tns_alias = rb.getString("tns_alias");
				} else {
					logger.warn("Can not find the 'tns_alias' property in database.properties");
				}
			} catch (Exception e) {
				logger.info("tns_alias is null", e);
			}
			if (this.tns_alias == null) {
				this.tns_alias = "";
			}
			logger.info("database url is " + this.url);
			logger.info("database driver is " + this.driver);
			logger.info("database user is " + this.user);
			logger.info("database password is " + this.password);
			if (!this.tns_alias.equals("")) {
				logger.info("database tns_alias is " + this.tns_alias);
			}

			BasicDataSource ds = new BasicDataSource();
			try {
				ds.setDriverClassName(this.driver);
				ds.setUrl(this.url);
				ds.setUsername(this.user);
				ds.setPassword(this.password);
				ds.setMaxActive(this.load_works + 10);
				ds.setInitialSize(this.load_works / 2);
				ds.setMaxWait(60000);
				ds.setMaxIdle(this.load_works / 2);
				ds.setMinIdle(5);
			} catch (Exception e) {
				logger.error("create basic DataSource error:", e);
				return false;
			}
			LoadMaster.dataSource = ds;
			return true;
		} catch (Exception e) {
			logger.error("dataload.properties not proper config exit program!!!", e);
			return false;
		} finally {
		}
	}

	public boolean initConfig() {
		if (this.loadDatabaseConfig() == true) {
			if (this.loadDataloadConfig() == true) {
				return true;
			} else {
				logger.error("load dataload config failure");
			}
		} else {
			logger.error("load database config failure");
		}
		return false;

	}

	public static void main(String args[]) {
		PropertyConfigurator.configure(System.getProperty("user.dir") + File.separator + "conf"
				+ File.separator + "log4j.properties");
		LoadMasterConfig lmc = new LoadMasterConfig();
		lmc.initConfig();
	}
}
