package com.ztesoft.cep.model;

import java.io.File;
import java.util.*;

import com.ztesoft.cep.LoadMaster;

import org.apache.log4j.Logger;

public class LoadTask {
	private long id;

	private int eventtype;

	private Date contentTimestamp;

	private String destFilePath;

	private long destFileSize;

	private TASKSTATE state;
	// use this field to store exception info when get a exception
	private String stateInfo;

	private Date taskGenTime;

	private Date taskLastExecStartTime;

	private Date taskLastExecEndTime;
	// 表名 用来冗余
	private String destTablename;
	// use this filed to store load schema
	private String loadSchema;// 如果操作成功 那么不设置 否则设置

	private File commadnFile;

	private boolean isNewTask = true;

	public boolean isNewTask() {
		return isNewTask;
	}

	public void setNewTask(boolean isNewTask) {
		this.isNewTask = isNewTask;
	}

	public File getCommadnFile() {
		return commadnFile;
	}

	public void setCommadnFile(File commadnFile) {
		this.commadnFile = commadnFile;
	}

	public String getLoadSchema() {
		return loadSchema;
	}

	public void setLoadSchema(String loadSchema) {
		this.loadSchema = loadSchema;
	}

	private LoadConfig config;// 数据库里面的导入配置

	static Logger logger = Logger.getLogger(LoadMaster.class.getName());

	public Date getContentTimestamp() {
		return contentTimestamp;
	}

	public void setContentTimestamp(Date contentTimestamp) {
		this.contentTimestamp = contentTimestamp;
	}

	public String getStateInfo() {
		if (stateInfo == null) {
			return STATEINFOSTR[state.ordinal()];
		}
		return stateInfo;
	}

	public void setStateInfo(String stateInfo) {
		this.stateInfo = stateInfo;
	}

	public Date getTaskLastExecStartTime() {
		return taskLastExecStartTime;
	}

	public void setTaskLastExecStartTime(Date taskLastExecStartTime) {
		this.taskLastExecStartTime = taskLastExecStartTime;
	}

	public String getDestTablename() {
		return destTablename;
	}

	public void setDestTablename(String destTablename) {
		this.destTablename = destTablename;
	}

	public enum TASKSTATE {
		NEW, OPERATING, ERROR, SUCCESSFUL
	};

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	/*
	 * 用来得到配置的属性 分表的模式
	 */

	// tableSchema

	/*
	 * modify :2012-10-18 author :xqh 增加一个属性用来记录该数据文件入库的时间.
	 */

	public LoadConfig getConfig() {
		return config;
	}

	public void setConfig(LoadConfig config) {
		this.config = config;
	}

	public static Logger getLogger() {
		return logger;
	}

	public static void setLogger(Logger logger) {
		LoadTask.logger = logger;
	}

	public int getEventtype() {
		return eventtype;
	}

	public void setEventtype(int eventtype) {
		this.eventtype = eventtype;
	}

	public String getDestFilePath() {
		return destFilePath;
	}

	public void setDestFilePath(String destFilePath) {
		this.destFilePath = destFilePath;
	}

	public long getDestFileSize() {
		return destFileSize;
	}

	public void setDestFileSize(long destFileSize) {
		this.destFileSize = destFileSize;
	}

	public TASKSTATE getState() {
		return state;
	}

	public static String[] STATEINFOSTR = { "new task", "task operation in progress",
			"task exec error", "task exec sussessful", "error state" };

	public void setState(int i) {
		//
		switch (i) {
		case 0:
			this.state = TASKSTATE.NEW;
			break;
		case 1:
			this.state = TASKSTATE.OPERATING;
			break;
		case 2:
			this.state = TASKSTATE.ERROR;
			break;
		case 3:
			this.state = TASKSTATE.SUCCESSFUL;
			break;
		default:
			this.state = TASKSTATE.ERROR;
			break;
		}
	}

	public Date getTaskGenTime() {
		return taskGenTime;
	}

	public void setTaskGenTime(Date taskGenTime) {
		this.taskGenTime = taskGenTime;
	}

	public Date getTaskLastExecEndTime() {
		return taskLastExecEndTime;
	}

	public void setTaskLastExecEndTime(Date taskLastExecEndTime) {
		this.taskLastExecEndTime = taskLastExecEndTime;
	}

	public void setState(TASKSTATE result) {
		this.state = result;
	}

}
